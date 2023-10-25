package resources

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/Snowflake-Labs/terraform-provider-snowflake/pkg/helpers"
	"github.com/Snowflake-Labs/terraform-provider-snowflake/pkg/sdk"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"golang.org/x/exp/slices"
)

var grantPrivilegesToDatabaseRoleSchema = map[string]*schema.Schema{
	"privileges": {
		Type:        schema.TypeSet,
		Optional:    true,
		Description: "The privileges to grant on the database role.",
		Elem: &schema.Schema{
			Type: schema.TypeString,
		},
		ConflictsWith: []string{
			"all_privileges",
		},
	},
	"all_privileges": {
		Type:        schema.TypeBool,
		Optional:    true,
		Default:     false,
		Description: "Grant all privileges on the database role.",
		ConflictsWith: []string{
			"privileges",
			"on_database",
		},
	},
	"on_database": {
		Type:          schema.TypeBool,
		Optional:      true,
		Default:       false,
		Description:   "If true, the privileges will be granted on the database.",
		ConflictsWith: []string{"on_schema", "on_schema_object", "all_privileges"},
		ForceNew:      true,
	},
	"on_schema": {
		Type:          schema.TypeList,
		Optional:      true,
		MaxItems:      1,
		ConflictsWith: []string{"on_database", "on_schema_object"},
		Description:   "Specifies the schema on which privileges will be granted.",
		ForceNew:      true,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"schema_name": {
					Type:          schema.TypeString,
					Optional:      true,
					Description:   "The fully qualified name of the schema.",
					ConflictsWith: []string{"on_schema.0.all_schemas", "on_schema.0.future_schemas"},
					ForceNew:      true,
				},
				"all_schemas": {
					Type:          schema.TypeBool,
					Optional:      true,
					Description:   "Grant privileges to all schemas.",
					ConflictsWith: []string{"on_schema.0.schema_name", "on_schema.0.future_schemas"},
					ForceNew:      true,
				},
				"future_schemas": {
					Type:          schema.TypeBool,
					Optional:      true,
					Description:   "Grant privileges to future schemas.",
					ConflictsWith: []string{"on_schema.0.schema_name", "on_schema.0.all_schemas"},
					ForceNew:      true,
				},
			},
		},
	},
	"on_schema_object": {
		Type:          schema.TypeList,
		Optional:      true,
		MaxItems:      1,
		ConflictsWith: []string{"on_database", "on_schema"},
		Description:   "Specifies the schema object on which privileges will be granted.",
		ForceNew:      true,
		Elem: &schema.Resource{
			Schema: map[string]*schema.Schema{
				"object_type": {
					Type:          schema.TypeString,
					Optional:      true,
					Description:   "The object type of the schema object on which privileges will be granted. Valid values are: ALERT | DYNAMIC TABLE | EVENT TABLE | FILE FORMAT | FUNCTION | PROCEDURE | SECRET | SEQUENCE | PIPE | MASKING POLICY | PASSWORD POLICY | ROW ACCESS POLICY | SESSION POLICY | TAG | STAGE | STREAM | TABLE | EXTERNAL TABLE | TASK | VIEW | MATERIALIZED VIEW",
					RequiredWith:  []string{"on_schema_object.0.object_name"},
					ConflictsWith: []string{"on_schema_object.0.all", "on_schema_object.0.future"},
					ForceNew:      true,
					ValidateFunc: validation.StringInSlice([]string{
						"ALERT",
						"DYNAMIC TABLE",
						"EVENT TABLE",
						"FILE FORMAT",
						"FUNCTION",
						"PROCEDURE",
						"SECRET",
						"SEQUENCE",
						"PIPE",
						"MASKING POLICY",
						"PASSWORD POLICY",
						"ROW ACCESS POLICY",
						"SESSION POLICY",
						"TAG",
						"STAGE",
						"STREAM",
						"TABLE",
						"EXTERNAL TABLE",
						"TASK",
						"VIEW",
						"MATERIALIZED VIEW",
					}, true),
				},
				"object_name": {
					Type:          schema.TypeString,
					Optional:      true,
					Description:   "The fully qualified name of the object on which privileges will be granted.",
					RequiredWith:  []string{"on_schema_object.0.object_type"},
					ConflictsWith: []string{"on_schema_object.0.all", "on_schema_object.0.future"},
					ForceNew:      true,
				},
				"all": {
					Type:        schema.TypeList,
					Optional:    true,
					MaxItems:    1,
					Description: "Configures the privilege to be granted on all objects in eihter a database or schema.",
					ForceNew:    true,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"object_type_plural": {
								Type:        schema.TypeString,
								Required:    true,
								Description: "The plural object type of the schema object on which privileges will be granted. Valid values are: ALERTS | DYNAMIC TABLES | EVENT TABLES | FILE FORMATS | FUNCTIONS | PROCEDURES | SECRETS | SEQUENCES | PIPES | MASKING POLICIES | PASSWORD POLICIES | ROW ACCESS POLICIES | SESSION POLICIES | TAGS | STAGES | STREAMS | TABLES | EXTERNAL TABLES | TASKS | VIEWS | MATERIALIZED VIEWS",
								ForceNew:    true,
								ValidateFunc: validation.StringInSlice([]string{
									"ALERTS",
									"DYNAMIC TABLES",
									"EVENT TABLES",
									"FILE FORMATS",
									"FUNCTIONS",
									"PROCEDURES",
									"SECRETS",
									"SEQUENCES",
									"PIPES",
									"MASKING POLICIES",
									"PASSWORD POLICIES",
									"ROW ACCESS POLICIES",
									"SESSION POLICIES",
									"TAGS",
									"STAGES",
									"STREAMS",
									"TABLES",
									"EXTERNAL TABLES",
									"TASKS",
									"VIEWS",
									"MATERIALIZED VIEWS",
								}, true),
							},
							"in_database": {
								Type:          schema.TypeBool,
								Optional:      true,
								Description:   "Grant privileges for the entire database.",
								ConflictsWith: []string{"on_schema_object.0.all.in_schema"},
								ForceNew:      true,
							},
							"in_schema": {
								Type:          schema.TypeString,
								Optional:      true,
								Description:   "The fully qualified name of the schema.",
								ConflictsWith: []string{"on_schema_object.0.all.in_database"},
								ForceNew:      true,
							},
						},
					},
				},
				"future": {
					Type:        schema.TypeList,
					Optional:    true,
					MaxItems:    1,
					Description: "Configures the privilege to be granted on future objects in eihter a database or schema.",
					ForceNew:    true,
					Elem: &schema.Resource{
						Schema: map[string]*schema.Schema{
							"object_type_plural": {
								Type:        schema.TypeString,
								Required:    true,
								Description: "The plural object type of the schema object on which privileges will be granted. Valid values are: ALERTS | DYNAMIC TABLES | EVENT TABLES | FILE FORMATS | FUNCTIONS | PROCEDURES | SECRETS | SEQUENCES | PIPES | MASKING POLICIES | PASSWORD POLICIES | ROW ACCESS POLICIES | SESSION POLICIES | TAGS | STAGES | STREAMS | TABLES | EXTERNAL TABLES | TASKS | VIEWS | MATERIALIZED VIEWS",
								ForceNew:    true,
								ValidateFunc: validation.StringInSlice([]string{
									"ALERTS",
									"DYNAMIC TABLES",
									"EVENT TABLES",
									"FILE FORMATS",
									"FUNCTIONS",
									"PROCEDURES",
									"SECRETS",
									"SEQUENCES",
									"PIPES",
									"MASKING POLICIES",
									"PASSWORD POLICIES",
									"ROW ACCESS POLICIES",
									"SESSION POLICIES",
									"TAGS",
									"STAGES",
									"STREAMS",
									"TABLES",
									"EXTERNAL TABLES",
									"TASKS",
									"VIEWS",
									"MATERIALIZED VIEWS",
								}, true),
							},
							"in_schema": {
								Type:          schema.TypeString,
								Optional:      true,
								Description:   "The fully qualified name of the schema.",
								ConflictsWith: []string{"on_schema_object.0.all.in_database"},
								ForceNew:      true,
							},
						},
					},
				},
			},
		},
	},
	"role_name": {
		Type:        schema.TypeString,
		Required:    true,
		Description: "The name of the database role to which privileges will be granted.",
		ForceNew:    true,
	},
	"database_name": {
		Type:        schema.TypeString,
		Required:    true,
		Description: "The name of the database in which the database role exists.",
		ForceNew:    true,
	},
	"with_grant_option": {
		Type:        schema.TypeBool,
		Optional:    true,
		Description: "Specifies whether the grantee can grant the privileges to other users.",
		Default:     false,
		ForceNew:    true,
	},
}

func GrantPrivilegesToDatabaseRole() *schema.Resource {
	return &schema.Resource{
		Create: CreateGrantPrivilegesToDatabaseRole,
		Read:   ReadGrantPrivilegesToDatabaseRole,
		Delete: DeleteGrantPrivilegesToDatabaseRole,
		Update: UpdateGrantPrivilegesToDatabaseRole,

		Schema: grantPrivilegesToDatabaseRoleSchema,
		Importer: &schema.ResourceImporter{
			StateContext: func(ctx context.Context, d *schema.ResourceData, m interface{}) ([]*schema.ResourceData, error) {
				resourceID := NewGrantPrivilegesToDatabaseRoleID(d.Id())
				if err := d.Set("role_name", resourceID.RoleName); err != nil {
					return nil, err
				}
				if err := d.Set("database_name", resourceID.DatabaseName); err != nil {
					return nil, err
				}
				if err := d.Set("privileges", resourceID.Privileges); err != nil {
					return nil, err
				}
				if err := d.Set("all_privileges", resourceID.AllPrivileges); err != nil {
					return nil, err
				}
				if err := d.Set("with_grant_option", resourceID.WithGrantOption); err != nil {
					return nil, err
				}
				if err := d.Set("on_database", resourceID.OnDatabase); err != nil {
					return nil, err
				}
				if resourceID.OnSchema {
					var onSchema []interface{}
					if resourceID.SchemaName != "" {
						onSchema = append(onSchema, map[string]interface{}{
							"schema_name": resourceID.SchemaName,
						})
					}
					if resourceID.All {
						onSchema = append(onSchema, map[string]interface{}{
							"all_schemas": true,
						})
					}
					if resourceID.Future {
						onSchema = append(onSchema, map[string]interface{}{
							"future_schemas": true,
						})
					}
					if err := d.Set("on_schema", onSchema); err != nil {
						return nil, err
					}
				}

				if resourceID.OnSchemaObject {
					var onSchemaObject []interface{}
					if resourceID.ObjectName != "" {
						onSchemaObject = append(onSchemaObject, map[string]interface{}{
							"object_name": resourceID.ObjectName,
							"object_type": resourceID.ObjectType,
						})
					}
					if resourceID.All {
						all := make([]interface{}, 0)
						m := map[string]interface{}{
							"object_type_plural": resourceID.ObjectTypePlural,
						}

						if resourceID.InSchema {
							m["in_schema"] = resourceID.SchemaName
						}
						m["in_database"] = resourceID.InDatabase
						all = append(all, m)
						onSchemaObject = append(onSchemaObject, map[string]interface{}{
							"all": all,
						})
					}
					if resourceID.Future {
						future := make([]interface{}, 0)
						m := map[string]interface{}{
							"object_type_plural": resourceID.ObjectTypePlural,
						}
						if resourceID.InSchema {
							m["in_schema"] = resourceID.SchemaName
						}
						m["in_database"] = resourceID.InDatabase
						future = append(future, m)
						onSchemaObject = append(onSchemaObject, map[string]interface{}{
							"future": future,
						})
					}
					if err := d.Set("on_schema_object", onSchemaObject); err != nil {
						return nil, err
					}
				}

				return []*schema.ResourceData{d}, nil
			},
		},
	}
}

// we need to keep track of literally everything to construct a unique identifier that can be imported
type GrantPrivilegesToDatabaseRoleID struct {
	RoleName         string
	DatabaseName     string
	Privileges       []string
	AllPrivileges    bool
	WithGrantOption  bool
	OnDatabase       bool
	OnSchema         bool
	OnSchemaObject   bool
	All              bool
	Future           bool
	ObjectType       string
	ObjectName       string
	ObjectTypePlural string
	InSchema         bool
	SchemaName       string
	InDatabase       bool
}

func NewGrantPrivilegesToDatabaseRoleID(id string) GrantPrivilegesToDatabaseRoleID {
	parts := strings.Split(id, "|")
	privileges := strings.Split(parts[2], ",")
	if len(privileges) == 1 && privileges[0] == "" {
		privileges = []string{}
	}
	return GrantPrivilegesToDatabaseRoleID{
		RoleName:         parts[0],
		DatabaseName:     parts[1],
		Privileges:       privileges,
		AllPrivileges:    parts[3] == "true",
		WithGrantOption:  parts[4] == "true",
		OnDatabase:       parts[5] == "true",
		OnSchema:         parts[6] == "true",
		OnSchemaObject:   parts[7] == "true",
		All:              parts[8] == "true",
		Future:           parts[9] == "true",
		ObjectType:       parts[10],
		ObjectName:       parts[11],
		ObjectTypePlural: parts[12],
		InSchema:         parts[13] == "true",
		SchemaName:       parts[14],
	}
}

func (v GrantPrivilegesToDatabaseRoleID) String() string {
	return helpers.EncodeSnowflakeID(v.RoleName, v.DatabaseName, v.Privileges, v.AllPrivileges, v.WithGrantOption, v.OnDatabase, v.OnSchema, v.OnSchemaObject, v.All, v.Future, v.ObjectType, v.ObjectName, v.ObjectTypePlural, v.InSchema, v.SchemaName)
}

func CreateGrantPrivilegesToDatabaseRole(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	client := sdk.NewClientFromDB(db)
	ctx := context.Background()
	resourceID := &GrantPrivilegesToDatabaseRoleID{}
	var privileges []string
	if p, ok := d.GetOk("privileges"); ok {
		privileges = expandStringList(p.(*schema.Set).List())
		resourceID.Privileges = privileges
	}
	allPrivileges := d.Get("all_privileges").(bool)
	resourceID.AllPrivileges = allPrivileges
	databaseName := d.Get("database_name").(string)
	resourceID.DatabaseName = databaseName
	privilegesToGrant, on, err := configureDatabaseRoleGrantPrivilegeOptions(d, privileges, allPrivileges, resourceID)
	if err != nil {
		return fmt.Errorf("error configuring database role grant privilege options: %w", err)
	}
	withGrantOption := d.Get("with_grant_option").(bool)
	resourceID.WithGrantOption = withGrantOption
	opts := sdk.GrantPrivilegesToDatabaseRoleOptions{
		WithGrantOption: sdk.Bool(withGrantOption),
	}
	roleName := d.Get("role_name").(string)
	resourceID.RoleName = roleName
	roleID := sdk.NewDatabaseObjectIdentifier(databaseName, roleName)
	err = client.Grants.GrantPrivilegesToDatabaseRole(ctx, privilegesToGrant, on, roleID, &opts)
	if err != nil {
		return fmt.Errorf("error granting privileges to database role: %w", err)
	}

	d.SetId(resourceID.String())
	return ReadGrantPrivilegesToDatabaseRole(d, meta)
}

func ReadGrantPrivilegesToDatabaseRole(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	client := sdk.NewClientFromDB(db)
	ctx := context.Background()
	resourceID := NewGrantPrivilegesToDatabaseRoleID(d.Id())
	roleName := resourceID.RoleName
	allPrivileges := resourceID.AllPrivileges
	if allPrivileges {
		log.Printf("[DEBUG] cannot read ALL PRIVILEGES on grant to role %s because this is not returned by API", roleName)
		return nil // cannot read all privileges because its not something returned by API. We can check only if specific privileges are granted to the role
	}
	var opts sdk.ShowGrantOptions
	var grantOn sdk.ObjectType
	if resourceID.OnDatabase {
		grantOn = sdk.ObjectTypeDatabase
		opts = sdk.ShowGrantOptions{
			On: &sdk.ShowGrantsOn{
				Object: &sdk.Object{
					ObjectType: sdk.ObjectTypeDatabase,
					Name:       sdk.NewAccountObjectIdentifierFromFullyQualifiedName(resourceID.DatabaseName),
				},
			},
		}
	}

	if resourceID.OnSchema {
		grantOn = sdk.ObjectTypeSchema
		if resourceID.SchemaName != "" {
			opts = sdk.ShowGrantOptions{
				On: &sdk.ShowGrantsOn{
					Object: &sdk.Object{
						ObjectType: sdk.ObjectTypeSchema,
						Name:       sdk.NewDatabaseObjectIdentifier(resourceID.DatabaseName, resourceID.SchemaName),
					},
				},
			}
		}
		if resourceID.All {
			log.Printf("[DEBUG] cannot read ALL SCHEMAS IN DATABASE on grant to role %s because this is not returned by API", roleName)
			return nil // on_all is not supported by API
		}
		if resourceID.Future {
			opts = sdk.ShowGrantOptions{
				Future: sdk.Bool(true),
				In: &sdk.ShowGrantsIn{
					Database: sdk.Pointer(sdk.NewAccountObjectIdentifierFromFullyQualifiedName(resourceID.DatabaseName)),
				},
			}
		}
	}

	if resourceID.OnSchemaObject {
		if resourceID.ObjectName != "" {
			objectType := sdk.ObjectType(resourceID.ObjectType)
			grantOn = objectType
			opts = sdk.ShowGrantOptions{
				On: &sdk.ShowGrantsOn{
					Object: &sdk.Object{
						ObjectType: objectType,
						Name:       sdk.NewSchemaObjectIdentifierFromFullyQualifiedName(resourceID.DatabaseName + "." + resourceID.ObjectName),
					},
				},
			}
		}

		if resourceID.All {
			return nil // on_all is not supported by API
		}

		if resourceID.Future {
			grantOn = sdk.PluralObjectType(resourceID.ObjectTypePlural).Singular()
			if resourceID.InSchema {
				opts = sdk.ShowGrantOptions{
					Future: sdk.Bool(true),
					In: &sdk.ShowGrantsIn{
						Schema: sdk.Pointer(sdk.NewDatabaseObjectIdentifier(resourceID.DatabaseName, resourceID.SchemaName)),
					},
				}
			}
			opts = sdk.ShowGrantOptions{
				Future: sdk.Bool(true),
				In: &sdk.ShowGrantsIn{
					Database: sdk.Pointer(sdk.NewAccountObjectIdentifierFromFullyQualifiedName(resourceID.DatabaseName)),
				},
			}
		}
	}

	err := readDatabaseRoleGrantPrivileges(ctx, client, grantOn, resourceID, &opts, d)
	if err != nil {
		return err
	}
	return nil
}

func UpdateGrantPrivilegesToDatabaseRole(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	client := sdk.NewClientFromDB(db)
	ctx := context.Background()

	// the only thing that can change is "privileges"
	roleName := d.Get("role_name").(string)
	databaseName := d.Get("database_name").(string)
	roleID := sdk.NewDatabaseObjectIdentifier(databaseName, roleName)

	if d.HasChange("privileges") {
		old, new := d.GetChange("privileges")
		oldPrivileges := expandStringList(old.(*schema.Set).List())
		newPrivileges := expandStringList(new.(*schema.Set).List())

		addPrivileges := []string{}
		removePrivileges := []string{}
		for _, oldPrivilege := range oldPrivileges {
			if !slices.Contains(newPrivileges, oldPrivilege) {
				removePrivileges = append(removePrivileges, oldPrivilege)
			}
		}

		for _, newPrivilege := range newPrivileges {
			if !slices.Contains(oldPrivileges, newPrivilege) {
				addPrivileges = append(addPrivileges, newPrivilege)
			}
		}

		// first add new privileges
		if len(addPrivileges) > 0 {
			privilegesToGrant, on, err := configureDatabaseRoleGrantPrivilegeOptions(d, addPrivileges, false, &GrantPrivilegesToDatabaseRoleID{})
			if err != nil {
				return fmt.Errorf("error configuring database role grant privilege options: %w", err)
			}
			err = client.Grants.GrantPrivilegesToDatabaseRole(ctx, privilegesToGrant, on, roleID, nil)
			if err != nil {
				return fmt.Errorf("error granting privileges to database role: %w", err)
			}
		}

		// then remove old privileges
		if len(removePrivileges) > 0 {
			privilegesToRevoke, on, err := configureDatabaseRoleGrantPrivilegeOptions(d, removePrivileges, false, &GrantPrivilegesToDatabaseRoleID{})
			if err != nil {
				return fmt.Errorf("error configuring database role grant privilege options: %w", err)
			}
			err = client.Grants.RevokePrivilegesFromDatabaseRole(ctx, privilegesToRevoke, on, roleID, nil)
			if err != nil {
				return fmt.Errorf("error revoking privileges from database role: %w", err)
			}
		}
		resourceID := NewGrantPrivilegesToDatabaseRoleID(d.Id())
		resourceID.Privileges = newPrivileges
		d.SetId(resourceID.String())
	}
	return ReadGrantPrivilegesToDatabaseRole(d, meta)
}

func DeleteGrantPrivilegesToDatabaseRole(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	client := sdk.NewClientFromDB(db)
	ctx := context.Background()

	roleName := d.Get("role_name").(string)
	databaseName := d.Get("database_name").(string)
	roleID := sdk.NewDatabaseObjectIdentifier(databaseName, roleName)

	var privileges []string
	if p, ok := d.GetOk("privileges"); ok {
		privileges = expandStringList(p.(*schema.Set).List())
	}
	allPrivileges := d.Get("all_privileges").(bool)
	privilegesToRevoke, on, err := configureDatabaseRoleGrantPrivilegeOptions(d, privileges, allPrivileges, &GrantPrivilegesToDatabaseRoleID{})
	if err != nil {
		return fmt.Errorf("error configuring database role grant privilege options: %w", err)
	}

	err = client.Grants.RevokePrivilegesFromDatabaseRole(ctx, privilegesToRevoke, on, roleID, nil)
	if err != nil {
		return fmt.Errorf("error revoking privileges from database role: %w", err)
	}
	d.SetId("")
	return nil
}

func configureDatabaseRoleGrantPrivilegeOptions(d *schema.ResourceData, privileges []string, allPrivileges bool, resourceID *GrantPrivilegesToDatabaseRoleID) (*sdk.DatabaseRoleGrantPrivileges, *sdk.DatabaseRoleGrantOn, error) {
	var privilegesToGrant *sdk.DatabaseRoleGrantPrivileges
	on := sdk.DatabaseRoleGrantOn{}
	if v, ok := d.GetOk("on_database"); ok && v.(bool) {
		on.Database = sdk.Pointer(sdk.NewAccountObjectIdentifierFromFullyQualifiedName(resourceID.DatabaseName))
		resourceID.OnDatabase = true
		privilegesToGrant = setDatabaseRolePrivilegeOptions(privileges, allPrivileges, true, false, false)
		return privilegesToGrant, &on, nil
	}

	if v, ok := d.GetOk("on_schema"); ok && len(v.([]interface{})) > 0 {
		onSchema := v.([]interface{})[0].(map[string]interface{})
		on.Schema = &sdk.GrantOnSchema{}
		resourceID.OnSchema = true
		if v, ok := onSchema["schema_name"]; ok && len(v.(string)) > 0 {
			resourceID.SchemaName = v.(string)
			on.Schema.Schema = sdk.Pointer(sdk.NewDatabaseObjectIdentifier(resourceID.DatabaseName, v.(string)))
		}
		if v, ok := onSchema["all_schemas"]; ok && v.(bool) {
			resourceID.All = true
			resourceID.InDatabase = true
			on.Schema.AllSchemasInDatabase = sdk.Pointer(sdk.NewAccountObjectIdentifierFromFullyQualifiedName(resourceID.DatabaseName))
		}

		if v, ok := onSchema["future_schemas"]; ok && v.(bool) {
			resourceID.Future = true
			resourceID.InDatabase = true
			on.Schema.FutureSchemasInDatabase = sdk.Pointer(sdk.NewAccountObjectIdentifierFromFullyQualifiedName(resourceID.DatabaseName))
		}
		privilegesToGrant = setDatabaseRolePrivilegeOptions(privileges, allPrivileges, false, true, false)
		return privilegesToGrant, &on, nil
	}

	if v, ok := d.GetOk("on_schema_object"); ok && len(v.([]interface{})) > 0 {
		onSchemaObject := v.([]interface{})[0].(map[string]interface{})
		on.SchemaObject = &sdk.GrantOnSchemaObject{}
		resourceID.OnSchemaObject = true
		if v, ok := onSchemaObject["object_type"]; ok && len(v.(string)) > 0 {
			resourceID.ObjectType = v.(string)
			on.SchemaObject.SchemaObject = &sdk.Object{
				ObjectType: sdk.ObjectType(v.(string)),
			}
		}
		if v, ok := onSchemaObject["object_name"]; ok && len(v.(string)) > 0 {
			resourceID.ObjectName = v.(string)
			on.SchemaObject.SchemaObject.Name = sdk.Pointer(sdk.NewSchemaObjectIdentifierFromFullyQualifiedName(resourceID.DatabaseName + "." + v.(string)))
		}
		if v, ok := onSchemaObject["all"]; ok && len(v.([]interface{})) > 0 {
			all := v.([]interface{})[0].(map[string]interface{})
			on.SchemaObject.All = &sdk.GrantOnSchemaObjectIn{}
			resourceID.All = true
			pluralObjectType := all["object_type_plural"].(string)
			resourceID.ObjectTypePlural = pluralObjectType
			on.SchemaObject.All.PluralObjectType = sdk.PluralObjectType(pluralObjectType)
			if v, ok := all["in_database"]; ok && v.(bool) {
				resourceID.InDatabase = true
				on.SchemaObject.All.InDatabase = sdk.Pointer(sdk.NewAccountObjectIdentifierFromFullyQualifiedName(resourceID.DatabaseName))
			}
			if v, ok := all["in_schema"]; ok && len(v.(string)) > 0 {
				resourceID.InSchema = true
				resourceID.SchemaName = v.(string)
				on.SchemaObject.All.InSchema = sdk.Pointer(sdk.NewDatabaseObjectIdentifier(resourceID.DatabaseName, v.(string)))
			}
		}

		if v, ok := onSchemaObject["future"]; ok && len(v.([]interface{})) > 0 {
			future := v.([]interface{})[0].(map[string]interface{})
			resourceID.Future = true
			on.SchemaObject.Future = &sdk.GrantOnSchemaObjectIn{}
			pluralObjectType := future["object_type_plural"].(string)
			resourceID.ObjectTypePlural = pluralObjectType
			on.SchemaObject.Future.PluralObjectType = sdk.PluralObjectType(pluralObjectType)
			if v, ok := future["in_database"]; ok && v.(bool) {
				resourceID.InDatabase = true
				on.SchemaObject.Future.InDatabase = sdk.Pointer(sdk.NewAccountObjectIdentifierFromFullyQualifiedName(resourceID.DatabaseName))
			}
			if v, ok := future["in_schema"]; ok && len(v.(string)) > 0 {
				resourceID.InSchema = true
				resourceID.SchemaName = v.(string)
				on.SchemaObject.Future.InSchema = sdk.Pointer(sdk.NewDatabaseObjectIdentifier(resourceID.DatabaseName, v.(string)))
			}
		}

		privilegesToGrant = setDatabaseRolePrivilegeOptions(privileges, allPrivileges, false, false, true)
		return privilegesToGrant, &on, nil
	}
	return nil, nil, fmt.Errorf("invalid grant options")
}

func setDatabaseRolePrivilegeOptions(privileges []string, allPrivileges bool, onDatabase bool, onSchema bool, onSchemaObject bool) *sdk.DatabaseRoleGrantPrivileges {
	privilegesToGrant := &sdk.DatabaseRoleGrantPrivileges{}
	if allPrivileges {
		privilegesToGrant.AllPrivileges = sdk.Bool(true)
		return privilegesToGrant
	}
	if onDatabase {
		privilegesToGrant.DatabasePrivileges = []sdk.AccountObjectPrivilege{}
		for _, privilege := range privileges {
			privilegesToGrant.DatabasePrivileges = append(privilegesToGrant.DatabasePrivileges, sdk.AccountObjectPrivilege(privilege))
		}
		return privilegesToGrant
	}
	if onSchema {
		privilegesToGrant.SchemaPrivileges = []sdk.SchemaPrivilege{}
		for _, privilege := range privileges {
			privilegesToGrant.SchemaPrivileges = append(privilegesToGrant.SchemaPrivileges, sdk.SchemaPrivilege(privilege))
		}
		return privilegesToGrant
	}
	if onSchemaObject {
		privilegesToGrant.SchemaObjectPrivileges = []sdk.SchemaObjectPrivilege{}
		for _, privilege := range privileges {
			privilegesToGrant.SchemaObjectPrivileges = append(privilegesToGrant.SchemaObjectPrivileges, sdk.SchemaObjectPrivilege(privilege))
		}
		return privilegesToGrant
	}
	return nil
}

func readDatabaseRoleGrantPrivileges(ctx context.Context, client *sdk.Client, grantedOn sdk.ObjectType, id GrantPrivilegesToDatabaseRoleID, opts *sdk.ShowGrantOptions, d *schema.ResourceData) error {
	grants, err := client.Grants.Show(ctx, opts)
	if err != nil {
		return fmt.Errorf("error retrieving grants for database role: %w", err)
	}

	withGrantOption := d.Get("with_grant_option").(bool)
	privileges := []string{}
	roleName := d.Get("role_name").(string)

	for _, grant := range grants {
		// Only consider privileges that are already present in the ID so we
		// don't delete privileges managed by other resources.
		if !slices.Contains(id.Privileges, grant.Privilege) {
			continue
		}
		if grant.GrantOption == withGrantOption && grant.GranteeName.Name() == roleName {
			// future grants do not have grantedBy, only current grants do. If grantedby
			// is an empty string it means the grant could not have been created by terraform
			if !id.Future && grant.GrantedBy.Name() == "" {
				continue
			}
			// grant_on is for future grants, granted_on is for current grants. They function the same way though in a test for matching the object type
			if grantedOn == grant.GrantedOn || grantedOn == grant.GrantOn {
				privileges = append(privileges, grant.Privilege)
			}
		}
	}
	if err := d.Set("privileges", privileges); err != nil {
		return fmt.Errorf("error setting privileges for database role: %w", err)
	}
	return nil
}
