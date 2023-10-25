package snowflake

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/jmoiron/sqlx"
)

func NewDatabaseRoleBuilder(db *sql.DB, databaseName, roleName string) *DatabaseRoleBuilder {
	return &DatabaseRoleBuilder{
		db:           db,
		databaseName: databaseName,
		roleName:     roleName,
	}
}

type DatabaseRoleBuilder struct {
	databaseName string
	roleName     string
	comment      string
	db           *sql.DB
}

func (b *DatabaseRoleBuilder) WithName(databaseName, roleName string) *DatabaseRoleBuilder {
	b.databaseName = databaseName
	b.roleName = roleName
	return b
}

func (b *DatabaseRoleBuilder) WithComment(comment string) *DatabaseRoleBuilder {
	b.comment = comment
	return b
}

func (b *DatabaseRoleBuilder) Create() error {
	q := strings.Builder{}
	q.WriteString(fmt.Sprintf(`CREATE DATABASE ROLE "%s.%s"`, b.databaseName, b.roleName))
	if b.comment != "" {
		q.WriteString(fmt.Sprintf(" COMMENT = '%v'", b.comment))
	}
	_, err := b.db.Exec(q.String())
	return err
}

func (b *DatabaseRoleBuilder) SetComment(comment string) error {
	q := fmt.Sprintf(`ALTER DATABASE ROLE "%s.%s" SET COMMENT = '%v'`, b.databaseName, b.roleName, comment)
	_, err := b.db.Exec(q)
	return err
}

func (b *DatabaseRoleBuilder) UnsetComment() error {
	q := fmt.Sprintf(`ALTER DATABASE ROLE "%s.%s" UNSET COMMENT`, b.databaseName, b.roleName)
	_, err := b.db.Exec(q)
	return err
}

func (b *DatabaseRoleBuilder) Drop() error {
	q := fmt.Sprintf(`DROP DATABASE ROLE "%s.%s"`, b.databaseName, b.roleName)
	_, err := b.db.Exec(q)
	return err
}

func (b *DatabaseRoleBuilder) Show() (*DatabaseRole, error) {
	stmt := fmt.Sprintf(`SHOW DATABASE ROLES IN DATABASE "%s"`, b.databaseName)
	rows, err := Query(b.db, stmt)

	databaseRoles := []*DatabaseRole{}
	if err := sqlx.StructScan(rows, &databaseRoles); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			log.Println("[DEBUG] no database roles found")
			return nil, nil
		}
		return nil, fmt.Errorf("failed to scan stmt = %v err = %w", stmt, err)
	}
	for _, r := range databaseRoles {
		if r.Name.String == b.databaseName+"."+b.roleName {
			return r, err
		}
	}
	log.Println("[DEBUG] no database roles found")
	return nil, nil
}

func (b *DatabaseRoleBuilder) Rename(newName string) error {
	stmt := fmt.Sprintf(`ALTER DATABASE ROLE "%s.%s" RENAME TO "%s"`, b.databaseName, b.roleName, newName)
	_, err := b.db.Exec(stmt)
	return err
}

type DatabaseRole struct {
	Name    sql.NullString `db:"name"`
	Comment sql.NullString `db:"comment"`
	Owner   sql.NullString `db:"owner"`
}

func ListDatabaseRoles(db *sql.DB) ([]*DatabaseRole, error) {
	stmt := strings.Builder{}
	stmt.WriteString("SHOW DATABASE ROLES")
	rows, err := Query(db, stmt.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	databaseRoles := []*DatabaseRole{}
	if err := sqlx.StructScan(rows, &databaseRoles); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			log.Println("[DEBUG] no database roles found")
			return nil, nil
		}
		return nil, fmt.Errorf("failed to scan stmt = %v err = %w", stmt, err)
	}
	return databaseRoles, nil
}
