package resources_test

import (
	"database/sql"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/Snowflake-Labs/terraform-provider-snowflake/pkg/provider"
	"github.com/Snowflake-Labs/terraform-provider-snowflake/pkg/resources"
	. "github.com/Snowflake-Labs/terraform-provider-snowflake/pkg/testhelpers"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/stretchr/testify/require"
)

func TestDatabaseRoleGrants(t *testing.T) {
	r := require.New(t)
	err := resources.DatabaseRoleGrants().InternalValidate(provider.Provider().Schema, true)
	r.NoError(err)
}

func TestDatabaseRoleGrantsCreate(t *testing.T) {
	r := require.New(t)

	d := databaseRoleGrants(t, "good_name", map[string]interface{}{
		"database_name": "db_name",
		"role_name":     "good_name",
		"roles":         []interface{}{"role1", "role2"},
		"users":         []interface{}{"user1", "user2"},
	})

	WithMockDb(t, func(db *sql.DB, mock sqlmock.Sqlmock) {
		mock.ExpectExec(`GRANT DATABASE ROLE "db_name.good_name" TO ROLE "role2"`).WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec(`GRANT DATABASE ROLE "db_name.good_name" TO ROLE "role1"`).WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec(`GRANT DATABASE ROLE "db_name.good_name" TO USER "user1"`).WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec(`GRANT DATABASE ROLE "db_name.good_name" TO USER "user2"`).WillReturnResult(sqlmock.NewResult(1, 1))
		expectReadDatabaseRoleGrants(mock)
		err := resources.CreateDatabaseRoleGrants(d, db)
		r.NoError(err)
	})
}

func expectReadDatabaseRoleGrants(mock sqlmock.Sqlmock) {
	rows := sqlmock.NewRows([]string{
		"created_on",
		"role",
		"granted_to",
		"grantee_name",
		"granted_by",
	}).
		AddRow("_", "db_name.good_name", "ROLE", "role1", "").
		AddRow("_", "db_name.good_name", "ROLE", "role2", "").
		AddRow("_", "db_name.good_name", "USER", "user1", "").
		AddRow("_", "db_name.good_name", "USER", "user2", "")
	mock.ExpectQuery(`SHOW GRANTS OF DATABASE ROLE "db_name.good_name"`).WillReturnRows(rows)
}

func TestDatabaseRoleGrantsRead(t *testing.T) {
	r := require.New(t)

	d := databaseRoleGrants(t, "db_name|good_name||||role1,role2|false", map[string]interface{}{
		"database_name": "db_name",
		"role_name":     "good_name",
		"roles":         []interface{}{"role1", "role2"},
		"users":         []interface{}{"user1", "user2"},
	})

	WithMockDb(t, func(db *sql.DB, mock sqlmock.Sqlmock) {
		r.NotEmpty(d.State())
		expectReadDatabaseRoleGrants(mock)
		err := resources.ReadDatabaseRoleGrants(d, db)
		r.NotEmpty(d.State())
		r.NoError(err)
		r.Len(d.Get("users").(*schema.Set).List(), 2)
		r.Len(d.Get("roles").(*schema.Set).List(), 2)
	})
}

func TestDatabaseRoleGrantsDelete(t *testing.T) {
	r := require.New(t)

	d := databaseRoleGrants(t, "db_name|drop_it||||role1,role2|false", map[string]interface{}{
		"database_name": "db_name",
		"role_name":     "drop_it",
		"roles":         []interface{}{"role1", "role2"},
		"users":         []interface{}{"user1", "user2"},
	})

	WithMockDb(t, func(db *sql.DB, mock sqlmock.Sqlmock) {
		mock.ExpectExec(`REVOKE DATABASE ROLE "db_name.drop_it" FROM ROLE "role1"`).WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec(`REVOKE DATABASE ROLE "db_name.drop_it" FROM ROLE "role2"`).WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec(`REVOKE DATABASE ROLE "db_name.drop_it" FROM USER "user1"`).WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec(`REVOKE DATABASE ROLE "db_name.drop_it" FROM USER "user2"`).WillReturnResult(sqlmock.NewResult(1, 1))
		err := resources.DeleteDatabaseRoleGrants(d, db)
		r.NoError(err)
	})
}

func expectReadUnhandledDatabaseRoleGrants(mock sqlmock.Sqlmock) {
	rows := sqlmock.NewRows([]string{
		"created_on",
		"role",
		"granted_to",
		"grantee_name",
		"granted_by",
	}).
		AddRow("_", "db_name.good_name", "ROLE", "role1", "").
		AddRow("_", "db_name.good_name", "ROLE", "role2", "").
		AddRow("_", "db_name.good_name", "OTHER", "other1", "").
		AddRow("_", "db_name.good_name", "OTHER", "other2", "").
		AddRow("_", "db_name.good_name", "USER", "user1", "").
		AddRow("_", "db_name.good_name", "USER", "user2", "")
	mock.ExpectQuery(`SHOW GRANTS OF DATABASE ROLE "db_name.good_name"`).WillReturnRows(rows)
}

func TestIgnoreUnknownDatabaseRoleGrants(t *testing.T) {
	r := require.New(t)

	d := roleGrants(t, "db_name|good_name||||role1,role2|false", map[string]interface{}{
		"database_name": "db_name",
		"role_name":     "good_name",
		"roles":         []interface{}{"role1", "role2"},
		"users":         []interface{}{"user1", "user2"},
	})

	WithMockDb(t, func(db *sql.DB, mock sqlmock.Sqlmock) {
		// Make sure that extraneous grants are ignored.
		expectReadUnhandledDatabaseRoleGrants(mock)
		err := resources.ReadDatabaseRoleGrants(d, db)
		r.NoError(err)
		r.Len(d.Get("users").(*schema.Set).List(), 2)
		r.Len(d.Get("roles").(*schema.Set).List(), 2)
	})
}
