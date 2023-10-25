package resources_test

import (
	"fmt"
	"strings"
	"testing"

	acc "github.com/Snowflake-Labs/terraform-provider-snowflake/pkg/acceptance"
	"github.com/hashicorp/terraform-plugin-testing/helper/acctest"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

func TestAcc_GrantPrivilegesToDatabaseRole_onDatabase(t *testing.T) {
	name := strings.ToUpper(acctest.RandStringFromCharSet(10, acctest.CharSetAlpha))

	resource.ParallelTest(t, resource.TestCase{
		Providers:    acc.TestAccProviders(),
		PreCheck:     func() { acc.TestAccPreCheck(t) },
		CheckDestroy: nil,
		Steps: []resource.TestStep{
			{
				Config: grantPrivilegesToDatabaseRole_onDatabaseConfig(acc.TestDatabaseName, name, []string{"MONITOR USAGE"}),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "database_name", acc.TestDatabaseName),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "role_name", name),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_database", "true"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.#", "1"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.0", "MONITOR USAGE"),
				),
			},
			// ADD PRIVILEGE
			{
				Config: grantPrivilegesToDatabaseRole_onDatabaseConfig(acc.TestDatabaseName, name, []string{"MONITOR USAGE", "MANAGE GRANTS"}),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "database_name", acc.TestDatabaseName),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "role_name", name),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_database", "true"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.#", "2"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.0", "MANAGE GRANTS"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.1", "MONITOR USAGE"),
				),
			},
			// IMPORT
			{
				ResourceName:      "snowflake_grant_privileges_to_database_role.g",
				ImportState:       true,
				ImportStateVerify: true,
			},
		},
	})
}

func grantPrivilegesToDatabaseRole_onDatabaseConfig(database string, name string, privileges []string) string {
	doubleQuotePrivileges := make([]string, len(privileges))
	for i, p := range privileges {
		doubleQuotePrivileges[i] = fmt.Sprintf(`"%v"`, p)
	}
	privilegesString := strings.Join(doubleQuotePrivileges, ",")
	return fmt.Sprintf(`
	resource "snowflake_database_role" "r" {
		database = "%v"
		name = "%v"
	}

	resource "snowflake_grant_privileges_to_database_role" "g" {
		privileges = [%v]
		database_name = snowflake_database_role.r.database
		role_name  = snowflake_database_role.r.name
		on_database = true
	  }
	`, database, name, privilegesString)
}

func TestAcc_GrantPrivilegesToDatabaseRole_onSchema(t *testing.T) {
	name := strings.ToUpper(acctest.RandStringFromCharSet(10, acctest.CharSetAlpha))

	resource.ParallelTest(t, resource.TestCase{
		Providers:    acc.TestAccProviders(),
		PreCheck:     func() { acc.TestAccPreCheck(t) },
		CheckDestroy: nil,
		Steps: []resource.TestStep{
			{
				Config: grantPrivilegesToDatabaseRole_onSchemaConfig(acc.TestDatabaseName, name, []string{"MONITOR", "USAGE"}, acc.TestSchemaName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "database_name", acc.TestDatabaseName),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "role_name", name),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema.#", "1"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema.0.schema_name", fmt.Sprintf("\"%v\"", acc.TestSchemaName)),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.#", "2"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.0", "MONITOR"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.1", "USAGE"),
				),
			},
			// ADD PRIVILEGE
			{
				Config: grantPrivilegesToDatabaseRole_onSchemaConfig(acc.TestDatabaseName, name, []string{"MONITOR"}, acc.TestSchemaName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "database_name", acc.TestDatabaseName),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "role_name", name),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.#", "1"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.0", "MONITOR"),
				),
			},
			// IMPORT
			{
				ResourceName:      "snowflake_grant_privileges_to_database_role.g",
				ImportState:       true,
				ImportStateVerify: true,
			},
		},
	})
}

func grantPrivilegesToDatabaseRole_onSchemaConfig(database string, name string, privileges []string, schemaName string) string {
	doubleQuotePrivileges := make([]string, len(privileges))
	for i, p := range privileges {
		doubleQuotePrivileges[i] = fmt.Sprintf(`"%v"`, p)
	}
	privilegesString := strings.Join(doubleQuotePrivileges, ",")
	return fmt.Sprintf(`
	resource "snowflake_database_role" "r" {
		database = "%v"
		name = "%v"
	}

	resource "snowflake_grant_privileges_to_database_role" "g" {
		database_name = snowflake_database_role.r.database
		role_name = snowflake_database_role.r.name
		privileges = [%s]
		on_schema {
		  schema_name = "\"%s\""
		}
	}
	`, database, name, privilegesString, schemaName)
}

func grantPrivilegesToDatabaseRole_onSchemaConfigAllPrivileges(database string, name string, schemaName string) string {
	return fmt.Sprintf(`
	resource "snowflake_database_role" "r" {
		database = "%v"
		name = "%v"
	}

	resource "snowflake_grant_privileges_to_database_role" "g" {
		database_name = snowflake_database_role.r.database
		role_name = snowflake_database_role.r.name
		all_privileges = true
		on_schema {
			schema_name = "\"%s\""
		}
	}
	`, database, name, schemaName)
}

func TestAcc_GrantPrivilegesToDatabaseRole_onSchemaConfigAllPrivileges(t *testing.T) {
	name := strings.ToUpper(acctest.RandStringFromCharSet(10, acctest.CharSetAlpha))

	resource.ParallelTest(t, resource.TestCase{
		Providers:    acc.TestAccProviders(),
		PreCheck:     func() { acc.TestAccPreCheck(t) },
		CheckDestroy: nil,
		Steps: []resource.TestStep{
			{
				Config: grantPrivilegesToDatabaseRole_onSchemaConfigAllPrivileges(acc.TestDatabaseName, name, acc.TestSchemaName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "database_name", acc.TestDatabaseName),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "role_name", name),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema.#", "1"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema.0.schema_name", fmt.Sprintf("\"%v\"", acc.TestSchemaName)),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "all_privileges", "true"),
				),
			},
			// IMPORT
			{
				ResourceName:      "snowflake_grant_privileges_to_database_role.g",
				ImportState:       true,
				ImportStateVerify: true,
			},
		},
	})
}

func TestAcc_GrantPrivilegesToDatabaseRole_onSchema_allSchemasInDatabase(t *testing.T) {
	name := strings.ToUpper(acctest.RandStringFromCharSet(10, acctest.CharSetAlpha))

	resource.ParallelTest(t, resource.TestCase{
		Providers:    acc.TestAccProviders(),
		PreCheck:     func() { acc.TestAccPreCheck(t) },
		CheckDestroy: nil,
		Steps: []resource.TestStep{
			{
				Config: grantPrivilegesToDatabaseRole_onSchema_allSchemasInDatabaseConfig(acc.TestDatabaseName, name, []string{"MONITOR", "USAGE"}),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "database_name", acc.TestDatabaseName),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "role_name", name),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema.#", "1"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema.0.all_schemas_in_database", acc.TestDatabaseName),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.#", "2"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.0", "MONITOR"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.1", "USAGE"),
				),
			},
			// REMOVE PRIVILEGE
			{
				Config: grantPrivilegesToDatabaseRole_onSchema_allSchemasInDatabaseConfig(acc.TestDatabaseName, name, []string{"MONITOR"}),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "database_name", acc.TestDatabaseName),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "role_name", name),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.#", "1"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.0", "MONITOR"),
				),
			},
			// IMPORT
			{
				ResourceName:      "snowflake_grant_privileges_to_database_role.g",
				ImportState:       true,
				ImportStateVerify: true,
			},
		},
	})
}

func TestAcc_GrantPrivilegesToDatabaseRole_onSchema_futureSchemasInDatabase(t *testing.T) {
	name := strings.ToUpper(acctest.RandStringFromCharSet(10, acctest.CharSetAlpha))

	resource.ParallelTest(t, resource.TestCase{
		Providers:    acc.TestAccProviders(),
		PreCheck:     func() { acc.TestAccPreCheck(t) },
		CheckDestroy: nil,
		Steps: []resource.TestStep{
			{
				Config: grantPrivilegesToDatabaseRole_onSchema_futureSchemasInDatabaseConfig(acc.TestDatabaseName, name, []string{"MONITOR", "USAGE"}),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "database_name", acc.TestDatabaseName),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "role_name", name),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema.#", "1"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema.0.future_schemas_in_database", acc.TestDatabaseName),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.#", "2"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.0", "MONITOR"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.1", "USAGE"),
				),
			},
			// IMPORT
			{
				ResourceName:      "snowflake_grant_privileges_to_database_role.g",
				ImportState:       true,
				ImportStateVerify: true,
			},
		},
	})
}

func grantPrivilegesToDatabaseRole_onSchema_allSchemasInDatabaseConfig(database string, name string, privileges []string) string {
	doubleQuotePrivileges := make([]string, len(privileges))
	for i, p := range privileges {
		doubleQuotePrivileges[i] = fmt.Sprintf(`"%v"`, p)
	}
	privilegesString := strings.Join(doubleQuotePrivileges, ",")
	return fmt.Sprintf(`
	resource "snowflake_database_role" "r" {
		database = "%v"
		name = "%v"
	}

	resource "snowflake_grant_privileges_to_database_role" "g" {
		database_name = snowflake_database_role.r.database
		role_name = snowflake_database_role.r.name
		privileges = [%s]
		on_schema {
			all_schemas = true
		}
	}
	`, database, name, privilegesString)
}

func grantPrivilegesToDatabaseRole_onSchema_futureSchemasInDatabaseConfig(database string, name string, privileges []string) string {
	doubleQuotePrivileges := make([]string, len(privileges))
	for i, p := range privileges {
		doubleQuotePrivileges[i] = fmt.Sprintf(`"%v"`, p)
	}
	privilegesString := strings.Join(doubleQuotePrivileges, ",")
	return fmt.Sprintf(`
	resource "snowflake_database_role" "r" {
		database = "%v"
		name = "%v"
	}

	resource "snowflake_grant_privileges_to_database_role" "g" {
		database_name = snowflake_database_role.r.database
		role_name = snowflake_database_role.r.name
		privileges = [%s]
		on_schema {
			future_schemas = true
		}
	}
	`, database, name, privilegesString)
}

func TestAcc_GrantPrivilegesToDatabaseRole_onSchemaObject_objectType(t *testing.T) {
	name := strings.ToUpper(acctest.RandStringFromCharSet(10, acctest.CharSetAlpha))

	resource.ParallelTest(t, resource.TestCase{
		Providers:    acc.TestAccProviders(),
		PreCheck:     func() { acc.TestAccPreCheck(t) },
		CheckDestroy: nil,
		Steps: []resource.TestStep{
			{
				Config: grantPrivilegesToDatabaseRole_onSchemaObject_objectType(acc.TestDatabaseName, name, []string{"SELECT", "REFERENCES"}, acc.TestSchemaName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "database_name", acc.TestDatabaseName),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "role_name", name),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema_object.#", "1"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema_object.0.object_type", "VIEW"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema_object.0.object_name", fmt.Sprintf(`"%v"."%v"`, acc.TestSchemaName, name)),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.#", "2"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.0", "REFERENCES"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.1", "SELECT"),
				),
			},
			// REMOVE PRIVILEGE
			{
				Config: grantPrivilegesToDatabaseRole_onSchemaObject_objectType(acc.TestDatabaseName, name, []string{"SELECT"}, acc.TestSchemaName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "database_name", acc.TestDatabaseName),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "role_name", name),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.#", "1"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.0", "SELECT"),
				),
			},
			// IMPORT
			{
				ResourceName:      "snowflake_grant_privileges_to_database_role.g",
				ImportState:       true,
				ImportStateVerify: true,
			},
		},
	})
}

func grantPrivilegesToDatabaseRole_onSchemaObject_objectType(database string, name string, privileges []string, schemaName string) string {
	doubleQuotePrivileges := make([]string, len(privileges))
	for i, p := range privileges {
		doubleQuotePrivileges[i] = fmt.Sprintf(`"%v"`, p)
	}
	privilegesString := strings.Join(doubleQuotePrivileges, ",")
	return fmt.Sprintf(`
	resource "snowflake_database_role" "r" {
		database = "%v"
		name = "%v"
	}

	resource "snowflake_view" "v" {
		name        = "%v"
		database    = "%s"
		schema      = "%s"
		is_secure   = true
		statement   = "SELECT ROLE_NAME, ROLE_OWNER FROM INFORMATION_SCHEMA.APPLICABLE_ROLES"
	}

	resource "snowflake_grant_privileges_to_database_role" "g" {
		depends_on = [ snowflake_view.v]
		database_name = snowflake_database_role.r.database
		role_name = snowflake_database_role.r.name
		privileges = [%s]
		on_schema_object {
			object_type = "VIEW"
			object_name = "\"%s\".\"%s\""
		}
	}
	`, database, name, name, database, schemaName, privilegesString, schemaName, name)
}

func TestAcc_GrantPrivilegesToDatabaseRole_onSchemaObject_allInSchema(t *testing.T) {
	name := strings.ToUpper(acctest.RandStringFromCharSet(10, acctest.CharSetAlpha))

	resource.ParallelTest(t, resource.TestCase{
		Providers:    acc.TestAccProviders(),
		PreCheck:     func() { acc.TestAccPreCheck(t) },
		CheckDestroy: nil,
		Steps: []resource.TestStep{
			{
				Config: grantPrivilegesToDatabaseRole_onSchemaObject_allInSchema(acc.TestDatabaseName, name, []string{"SELECT", "REFERENCES"}, acc.TestSchemaName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "database_name", acc.TestDatabaseName),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "role_name", name),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema_object.#", "1"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema_object.0.all.#", "1"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema_object.0.all.0.object_type_plural", "TABLES"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema_object.0.all.0.in_schema", fmt.Sprintf(`"%v"`, acc.TestSchemaName)),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.#", "2"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.0", "REFERENCES"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.1", "SELECT"),
				),
			},
			// REMOVE PRIVILEGE
			{
				Config: grantPrivilegesToDatabaseRole_onSchemaObject_allInSchema(acc.TestDatabaseName, name, []string{"SELECT"}, acc.TestSchemaName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "database_name", acc.TestDatabaseName),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "role_name", name),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.#", "1"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.0", "SELECT"),
				),
			},
			// IMPORT
			{
				ResourceName:      "snowflake_grant_privileges_to_database_role.g",
				ImportState:       true,
				ImportStateVerify: true,
			},
		},
	})
}

func grantPrivilegesToDatabaseRole_onSchemaObject_allInSchema(database string, name string, privileges []string, schemaName string) string {
	doubleQuotePrivileges := make([]string, len(privileges))
	for i, p := range privileges {
		doubleQuotePrivileges[i] = fmt.Sprintf(`"%v"`, p)
	}
	privilegesString := strings.Join(doubleQuotePrivileges, ",")
	return fmt.Sprintf(`
	resource "snowflake_database_role" "r" {
		database = "%v"
		name = "%v"
	}

	resource "snowflake_grant_privileges_to_database_role" "g" {
		database_name = snowflake_database_role.r.database
		role_name = snowflake_database_role.r.name
		privileges = [%s]
		on_schema_object {
			all {
				object_type_plural = "TABLES"
				in_schema = "\"%s\""
			}
		}
	}
	`, database, name, privilegesString, schemaName)
}

func TestAcc_GrantPrivilegesToDatabaseRole_onSchemaObject_allInDatabase(t *testing.T) {
	name := strings.ToUpper(acctest.RandStringFromCharSet(10, acctest.CharSetAlpha))

	resource.ParallelTest(t, resource.TestCase{
		Providers:    acc.TestAccProviders(),
		PreCheck:     func() { acc.TestAccPreCheck(t) },
		CheckDestroy: nil,
		Steps: []resource.TestStep{
			{
				Config: grantPrivilegesToDatabaseRole_onSchemaObject_allInDatabase(acc.TestDatabaseName, name, []string{"SELECT", "REFERENCES"}),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "database_name", acc.TestDatabaseName),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "role_name", name),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema_object.#", "1"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema_object.0.all.#", "1"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema_object.0.all.0.object_type_plural", "TABLES"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema_object.0.all.0.in_database", acc.TestDatabaseName),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.#", "2"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.0", "REFERENCES"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.1", "SELECT"),
				),
			},
			// REMOVE PRIVILEGE
			{
				Config: grantPrivilegesToDatabaseRole_onSchemaObject_allInDatabase(acc.TestDatabaseName, name, []string{"SELECT"}),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "database_name", acc.TestDatabaseName),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "role_name", name),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.#", "1"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.0", "SELECT"),
				),
			},
			// IMPORT
			{
				ResourceName:      "snowflake_grant_privileges_to_database_role.g",
				ImportState:       true,
				ImportStateVerify: true,
			},
		},
	})
}

func grantPrivilegesToDatabaseRole_onSchemaObject_allInDatabase(database string, name string, privileges []string) string {
	doubleQuotePrivileges := make([]string, len(privileges))
	for i, p := range privileges {
		doubleQuotePrivileges[i] = fmt.Sprintf(`"%v"`, p)
	}
	privilegesString := strings.Join(doubleQuotePrivileges, ",")
	return fmt.Sprintf(`
	resource "snowflake_database_role" "r" {
		database = "%v"
		name = "%v"
	}

	resource "snowflake_grant_privileges_to_database_role" "g" {
		database_name = snowflake_database_role.r.database
		role_name = snowflake_database_role.r.name
		privileges = [%s]
		on_schema_object {
			all {
				object_type_plural = "TABLES"
				in_database = true
			}
		}
	}
	`, database, name, privilegesString)
}

func TestAcc_GrantPrivilegesToDatabaseRole_onSchemaObject_futureInSchema(t *testing.T) {
	name := strings.ToUpper(acctest.RandStringFromCharSet(10, acctest.CharSetAlpha))

	resource.ParallelTest(t, resource.TestCase{
		Providers:    acc.TestAccProviders(),
		PreCheck:     func() { acc.TestAccPreCheck(t) },
		CheckDestroy: nil,
		Steps: []resource.TestStep{
			{
				Config: grantPrivilegesToDatabaseRole_onSchemaObject_futureInSchema(acc.TestDatabaseName, name, []string{"SELECT", "REFERENCES"}, acc.TestSchemaName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "database_name", acc.TestDatabaseName),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "role_name", name),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema_object.#", "1"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema_object.0.future.#", "1"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema_object.0.future.0.object_type_plural", "TABLES"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema_object.0.future.0.in_schema", fmt.Sprintf(`"%v"`, acc.TestSchemaName)),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.#", "2"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.0", "REFERENCES"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.1", "SELECT"),
				),
			},
			// REMOVE PRIVILEGE
			{
				Config: grantPrivilegesToDatabaseRole_onSchemaObject_futureInSchema(acc.TestDatabaseName, name, []string{"SELECT"}, acc.TestSchemaName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "database_name", acc.TestDatabaseName),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "role_name", name),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.#", "1"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.0", "SELECT"),
				),
			},
			// IMPORT
			{
				ResourceName:      "snowflake_grant_privileges_to_database_role.g",
				ImportState:       true,
				ImportStateVerify: true,
			},
		},
	})
}

func grantPrivilegesToDatabaseRole_onSchemaObject_futureInSchema(database string, name string, privileges []string, schemaName string) string {
	doubleQuotePrivileges := make([]string, len(privileges))
	for i, p := range privileges {
		doubleQuotePrivileges[i] = fmt.Sprintf(`"%v"`, p)
	}
	privilegesString := strings.Join(doubleQuotePrivileges, ",")
	return fmt.Sprintf(`
	resource "snowflake_database_role" "r" {
		database = "%v"
		name = "%v"
	}

	resource "snowflake_grant_privileges_to_database_role" "g" {
		database_name = snowflake_database_role.r.database
		role_name = snowflake_database_role.r.name
		privileges = [%s]
		on_schema_object {
			future {
				object_type_plural = "TABLES"
				in_schema = "\"%s\""
			}
		}
	}
	`, database, name, privilegesString, schemaName)
}

func TestAcc_GrantPrivilegesToDatabaseRole_onSchemaObject_futureInDatabase(t *testing.T) {
	name := strings.ToUpper(acctest.RandStringFromCharSet(10, acctest.CharSetAlpha))
	objectType := "TABLES"
	resource.ParallelTest(t, resource.TestCase{
		Providers:    acc.TestAccProviders(),
		PreCheck:     func() { acc.TestAccPreCheck(t) },
		CheckDestroy: nil,
		Steps: []resource.TestStep{
			{
				Config: grantPrivilegesToDatabaseRole_onSchemaObject_futureInDatabase(acc.TestDatabaseName, name, objectType, []string{"SELECT", "REFERENCES"}),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "database_name", acc.TestDatabaseName),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "role_name", name),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema_object.#", "1"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema_object.0.future.#", "1"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema_object.0.future.0.object_type_plural", "TABLES"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema_object.0.future.0.in_database", acc.TestDatabaseName),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.#", "2"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.0", "REFERENCES"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.1", "SELECT"),
				),
			},
			// REMOVE PRIVILEGE
			{
				Config: grantPrivilegesToDatabaseRole_onSchemaObject_futureInDatabase(acc.TestDatabaseName, name, objectType, []string{"SELECT"}),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "database_name", acc.TestDatabaseName),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "role_name", name),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.#", "1"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.0", "SELECT"),
				),
			},
			// IMPORT
			{
				ResourceName:      "snowflake_grant_privileges_to_database_role.g",
				ImportState:       true,
				ImportStateVerify: true,
			},
		},
	})
}

func grantPrivilegesToDatabaseRole_onSchemaObject_futureInDatabase(database string, name string, objectType string, privileges []string) string {
	doubleQuotePrivileges := make([]string, len(privileges))
	for i, p := range privileges {
		doubleQuotePrivileges[i] = fmt.Sprintf(`"%v"`, p)
	}
	privilegesString := strings.Join(doubleQuotePrivileges, ",")
	return fmt.Sprintf(`
	resource "snowflake_database_role" "r" {
		database = "%v"
		name = "%v"
	}

	resource "snowflake_grant_privileges_to_database_role" "g" {
		database_name = snowflake_database_role.r.database
		role_name = snowflake_database_role.r.name
		privileges = [%s]
		on_schema_object {
			future {
				object_type_plural = "%s"
				in_database = true
			}
		}
	}
	`, database, name, privilegesString, objectType)
}

func TestAcc_GrantPrivilegesToDatabaseRole_multipleResources(t *testing.T) {
	name := strings.ToUpper(acctest.RandStringFromCharSet(10, acctest.CharSetAlpha))

	resource.ParallelTest(t, resource.TestCase{
		Providers:    acc.TestAccProviders(),
		PreCheck:     func() { acc.TestAccPreCheck(t) },
		CheckDestroy: nil,
		Steps: []resource.TestStep{
			{
				Config: grantPrivilegesToDatabaseRole_multipleResources(acc.TestDatabaseName, name, []string{"CREATE SCHEMA", "CREATE TABLE"}, []string{"MONITOR", "USAGE"}),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g1", "database_name", acc.TestDatabaseName),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g1", "role_name", name),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g1", "privileges.#", "2"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g1", "privileges.0", "CREATE SCHEMA"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g1", "privileges.1", "CREATE TABLE"),

					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g2", "database_name", acc.TestDatabaseName),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g2", "role_name", name),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g2", "privileges.#", "2"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g2", "privileges.0", "MONITOR"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g2", "privileges.1", "USAGE"),
				),
			},
			// IMPORT
			{
				ResourceName:      "snowflake_grant_privileges_to_database_role.g1",
				ImportState:       true,
				ImportStateVerify: true,
			},
			{
				ResourceName:      "snowflake_grant_privileges_to_database_role.g2",
				ImportState:       true,
				ImportStateVerify: true,
			},
		},
	})
}

func grantPrivilegesToDatabaseRole_multipleResources(database string, name string, privileges1, privileges2 []string) string {
	doubleQuotePrivileges1 := make([]string, len(privileges1))
	for i, p := range privileges1 {
		doubleQuotePrivileges1[i] = fmt.Sprintf(`"%v"`, p)
	}
	privilegesString1 := strings.Join(doubleQuotePrivileges1, ",")

	doubleQuotePrivileges2 := make([]string, len(privileges2))
	for i, p := range privileges2 {
		doubleQuotePrivileges2[i] = fmt.Sprintf(`"%v"`, p)
	}
	privilegesString2 := strings.Join(doubleQuotePrivileges2, ",")

	return fmt.Sprintf(`
	resource "snowflake_database_role" "r" {
		database = "%v"
		name = "%v"
	}

	resource "snowflake_grant_privileges_to_database_role" "g1" {
		role_name  = snowflake_role.r.name
		privileges = [%s]
		on_database = true
	}

	resource "snowflake_grant_privileges_to_database_role" "g2" {
		role_name  = snowflake_role.r.name
		privileges = [%s]
		on_database = true
	}
	`, database, name, privilegesString1, privilegesString2)
}

func TestAcc_GrantPrivilegesToDatabaseRole_onSchemaObject_futureInDatabase_externalTable(t *testing.T) {
	name := strings.ToUpper(acctest.RandStringFromCharSet(10, acctest.CharSetAlpha))
	objectType := "EXTERNAL TABLES"
	resource.ParallelTest(t, resource.TestCase{
		Providers:    acc.TestAccProviders(),
		PreCheck:     func() { acc.TestAccPreCheck(t) },
		CheckDestroy: nil,
		Steps: []resource.TestStep{
			{
				Config: grantPrivilegesToDatabaseRole_onSchemaObject_futureInDatabase(acc.TestDatabaseName, name, objectType, []string{"SELECT", "REFERENCES"}),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "database_name", acc.TestDatabaseName),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "role_name", name),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema_object.#", "1"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema_object.0.future.#", "1"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema_object.0.future.0.object_type_plural", "EXTERNAL TABLES"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "on_schema_object.0.future.0.in_database", acc.TestDatabaseName),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.#", "2"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.0", "REFERENCES"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.1", "SELECT"),
				),
			},
			// REMOVE PRIVILEGE
			{
				Config: grantPrivilegesToDatabaseRole_onSchemaObject_futureInDatabase(acc.TestDatabaseName, name, objectType, []string{"SELECT"}),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "database_name", acc.TestDatabaseName),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "role_name", name),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.#", "1"),
					resource.TestCheckResourceAttr("snowflake_grant_privileges_to_database_role.g", "privileges.0", "SELECT"),
				),
			},
			// IMPORT
			{
				ResourceName:      "snowflake_grant_privileges_to_database_role.g",
				ImportState:       true,
				ImportStateVerify: true,
			},
		},
	})
}
