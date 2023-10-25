package snowflake

import "fmt"

type DatabaseRoleGrantBuilder struct {
	databaseName string
	roleName     string
}

type DatabaseRoleGrantExecutable struct {
	databaseName string
	roleName     string
	granteeType  granteeType
	grantee      string
}

func DatabaseRoleGrant(databaseName, roleName string) *DatabaseRoleGrantBuilder {
	return &DatabaseRoleGrantBuilder{
		databaseName: databaseName,
		roleName:     roleName,
	}
}

func (gb *DatabaseRoleGrantBuilder) User(user string) *DatabaseRoleGrantExecutable {
	return &DatabaseRoleGrantExecutable{
		databaseName: gb.databaseName,
		roleName:     gb.roleName,
		granteeType:  userType,
		grantee:      user,
	}
}

func (gb *DatabaseRoleGrantBuilder) Role(role string) *DatabaseRoleGrantExecutable {
	return &DatabaseRoleGrantExecutable{
		databaseName: gb.databaseName,
		roleName:     gb.roleName,
		granteeType:  roleType,
		grantee:      role,
	}
}

func (gr *DatabaseRoleGrantExecutable) Grant() string {
	return fmt.Sprintf(`GRANT DATABASE ROLE "%s.%s" TO %s "%s"`, gr.databaseName, gr.roleName, gr.granteeType, gr.grantee) // nolint: gosec
}

func (gr *DatabaseRoleGrantExecutable) Revoke() string {
	return fmt.Sprintf(`REVOKE DATABASE ROLE "%s.%s" FROM %s "%s"`, gr.databaseName, gr.roleName, gr.granteeType, gr.grantee) // nolint: gosec
}
