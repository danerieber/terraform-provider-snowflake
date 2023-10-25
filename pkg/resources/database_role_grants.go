package resources

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/Snowflake-Labs/terraform-provider-snowflake/pkg/helpers"
	"github.com/Snowflake-Labs/terraform-provider-snowflake/pkg/snowflake"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/jmoiron/sqlx"
	"github.com/snowflakedb/gosnowflake"
	"golang.org/x/exp/slices"
)

func DatabaseRoleGrants() *schema.Resource {
	return &schema.Resource{
		Create: CreateDatabaseRoleGrants,
		Read:   ReadDatabaseRoleGrants,
		Delete: DeleteDatabaseRoleGrants,
		Update: UpdateDatabaseRoleGrants,

		Schema: map[string]*schema.Schema{
			"database_name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The name of the database in which the database role exists.",
				ForceNew:    true,
			},
			"role_name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The name of the database role we are granting.",
				ForceNew:    true,
			},
			"roles": {
				Type:        schema.TypeSet,
				Elem:        &schema.Schema{Type: schema.TypeString},
				Optional:    true,
				Description: "Grants role to this specified role.",
			},
			"users": {
				Type:        schema.TypeSet,
				Elem:        &schema.Schema{Type: schema.TypeString},
				Optional:    true,
				Description: "Grants role to this specified user.",
			},
			"enable_multiple_grants": {
				Type:        schema.TypeBool,
				Optional:    true,
				Description: "When this is set to true, multiple grants of the same type can be created. This will cause Terraform to not revoke grants applied to roles and objects outside Terraform.",
				Default:     false,
			},
		},

		Importer: &schema.ResourceImporter{
			StateContext: func(ctx context.Context, d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
				parts := strings.Split(d.Id(), helpers.IDDelimiter)
				if len(parts) != 3 {
					return nil, fmt.Errorf("invalid ID specified for role grants, expected {database_name}|{role_name}|{roles}|{users}, got %v", d.Id())
				}
				if err := d.Set("database_name", parts[0]); err != nil {
					return nil, err
				}
				if err := d.Set("role_name", parts[0]); err != nil {
					return nil, err
				}
				if err := d.Set("roles", helpers.StringListToList(parts[1])); err != nil {
					return nil, err
				}
				if err := d.Set("users", helpers.StringListToList(parts[2])); err != nil {
					return nil, err
				}
				return []*schema.ResourceData{d}, nil
			},
		},
	}
}

func CreateDatabaseRoleGrants(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	databaseName := d.Get("database_name").(string)
	roleName := d.Get("role_name").(string)
	roles := expandStringList(d.Get("roles").(*schema.Set).List())
	users := expandStringList(d.Get("users").(*schema.Set).List())

	if len(roles) == 0 && len(users) == 0 {
		return fmt.Errorf("no users or roles specified for database role grants")
	}

	grantID := helpers.EncodeSnowflakeID(databaseName, roleName, roles, users)
	d.SetId(grantID)

	for _, role := range roles {
		if err := grantDatabaseRoleToRole(db, databaseName, roleName, role); err != nil {
			return err
		}
	}

	for _, user := range users {
		if err := grantDatabaseRoleToUser(db, databaseName, roleName, user); err != nil {
			return err
		}
	}

	return ReadDatabaseRoleGrants(d, meta)
}

func grantDatabaseRoleToRole(db *sql.DB, database, role1, role2 string) error {
	g := snowflake.DatabaseRoleGrant(database, role1)
	err := snowflake.Exec(db, g.Role(role2).Grant())
	return err
}

func grantDatabaseRoleToUser(db *sql.DB, database, role1, user string) error {
	g := snowflake.DatabaseRoleGrant(database, role1)
	err := snowflake.Exec(db, g.User(user).Grant())
	return err
}

type databaseRoleGrant struct {
	CreatedOn   sql.RawBytes   `db:"created_on"`
	Role        sql.NullString `db:"role"`
	GrantedTo   sql.NullString `db:"granted_to"`
	GranteeName sql.NullString `db:"grantee_name"`
	Grantedby   sql.NullString `db:"granted_by"`
}

func ReadDatabaseRoleGrants(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	databaseName := d.Get("database_name").(string)
	roleName := d.Get("role_name").(string)

	roles := make([]string, 0)
	users := make([]string, 0)

	builder := snowflake.NewDatabaseRoleBuilder(db, databaseName, roleName)
	_, err := builder.Show()
	if errors.Is(err, sql.ErrNoRows) {
		// If not found, mark resource to be removed from state file during apply or refresh
		log.Printf("[DEBUG] database role (%s) not found", roleName)
		d.SetId("")
		return nil
	}

	grants, err := readGrantsForDatabaseRole(db, databaseName, roleName)
	if err != nil {
		return err
	}

	for _, grant := range grants {
		switch grant.GrantedTo.String {
		case "ROLE":
			for _, tfRole := range d.Get("roles").(*schema.Set).List() {
				if tfRole == grant.GranteeName.String {
					roles = append(roles, grant.GranteeName.String)
				}
			}
		case "USER":
			for _, tfUser := range d.Get("users").(*schema.Set).List() {
				if tfUser == grant.GranteeName.String {
					users = append(users, grant.GranteeName.String)
				}
			}
		default:
			log.Printf("[WARN] Ignoring unknown grant type %s", grant.GrantedTo.String)
		}
	}

	if err := d.Set("roles", roles); err != nil {
		return err
	}
	if err := d.Set("users", users); err != nil {
		return err
	}

	grantID := helpers.EncodeSnowflakeID(databaseName, roleName, roles, users)
	if grantID != d.Id() {
		d.SetId(grantID)
	}
	return nil
}

func readGrantsForDatabaseRole(db *sql.DB, databaseName, roleName string) ([]*databaseRoleGrant, error) {
	sdb := sqlx.NewDb(db, "snowflake")

	stmt := fmt.Sprintf(`SHOW GRANTS OF DATABASE ROLE "%s.%s"`, databaseName, roleName)
	rows, err := sdb.Queryx(stmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	grants := make([]*databaseRoleGrant, 0)
	for rows.Next() {
		g := &databaseRoleGrant{}
		if err := rows.StructScan(g); err != nil {
			return nil, err
		}
		grants = append(grants, g)
	}

	for _, g := range grants {
		if g.GranteeName.Valid {
			s := g.GranteeName.String
			s = strings.TrimPrefix(s, `"`)
			s = strings.TrimSuffix(s, `"`)
			g.GranteeName = sql.NullString{String: s}
		}
	}

	return grants, nil
}

func DeleteDatabaseRoleGrants(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	databaseName := d.Get("database_name").(string)
	roleName := d.Get("role_name").(string)

	roles := expandStringList(d.Get("roles").(*schema.Set).List())
	users := expandStringList(d.Get("users").(*schema.Set).List())

	for _, role := range roles {
		if err := revokeDatabaseRoleFromRole(db, databaseName, roleName, role); err != nil {
			return err
		}
	}

	for _, user := range users {
		if err := revokeDatabaseRoleFromUser(db, databaseName, roleName, user); err != nil {
			return err
		}
	}

	d.SetId("")
	return nil
}

func revokeDatabaseRoleFromRole(db *sql.DB, database, role1, role2 string) error {
	rg := snowflake.DatabaseRoleGrant(database, role1).Role(role2)
	err := snowflake.Exec(db, rg.Revoke())
	log.Printf("revokeRoleFromRole %v", err)
	if driverErr, ok := err.(*gosnowflake.SnowflakeError); ok { //nolint:errorlint // todo: should be fixed
		if driverErr.Number == 2003 {
			// handling error if a role has been deleted prior to revoking a role
			// 002003 (02000): SQL compilation error:
			// User 'XXX' does not exist or not authorized.
			roles, _ := snowflake.ListRoles(db, role2)
			roleNames := make([]string, len(roles))
			for i, r := range roles {
				roleNames[i] = r.Name.String
			}
			if !slices.Contains(roleNames, role2) {
				log.Printf("[WARN] Role %s does not exist. No need to revoke database role %s", role2, role1)
				return nil
			}
		}
	}
	return err
}

func revokeDatabaseRoleFromUser(db *sql.DB, database, role1, user string) error {
	rg := snowflake.DatabaseRoleGrant(database, role1).User(user)
	err := snowflake.Exec(db, rg.Revoke())
	if driverErr, ok := err.(*gosnowflake.SnowflakeError); ok { //nolint:errorlint // todo: should be fixed
		// handling error if a user has been deleted prior to revoking a role
		// 002003 (02000): SQL compilation error:
		// User 'XXX' does not exist or not authorized.
		if driverErr.Number == 2003 {
			users, _ := snowflake.ListUsers(user, db)
			logins := make([]string, len(users))
			for i, u := range users {
				logins[i] = u.LoginName.String
			}
			if !snowflake.Contains(logins, user) {
				log.Printf("[WARN] User %s does not exist. No need to revoke database role %s", user, role1)
				return nil
			}
		}
	}
	return err
}

func UpdateDatabaseRoleGrants(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	databaseName := d.Get("database_name").(string)
	roleName := d.Get("role_name").(string)

	x := func(resource string, grant func(db *sql.DB, database string, role string, target string) error, revoke func(db *sql.DB, database string, role string, target string) error) error {
		o, n := d.GetChange(resource)

		if o == nil {
			o = new(schema.Set)
		}
		if n == nil {
			n = new(schema.Set)
		}
		os := o.(*schema.Set)
		ns := n.(*schema.Set)

		remove := expandStringList(os.Difference(ns).List())
		add := expandStringList(ns.Difference(os).List())

		for _, user := range remove {
			if err := revoke(db, databaseName, roleName, user); err != nil {
				return err
			}
		}
		for _, user := range add {
			if err := grant(db, databaseName, roleName, user); err != nil {
				return err
			}
		}
		return nil
	}

	if err := x("users", grantDatabaseRoleToUser, revokeDatabaseRoleFromUser); err != nil {
		return err
	}

	if err := x("roles", grantDatabaseRoleToRole, revokeDatabaseRoleFromRole); err != nil {
		return err
	}

	return ReadDatabaseRoleGrants(d, meta)
}
