package datasources

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/Snowflake-Labs/terraform-provider-snowflake/pkg/snowflake"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

var currentRoleSchema = map[string]*schema.Schema{
	"name": {
		Type:        schema.TypeString,
		Computed:    true,
		Description: "The name of the [primary role](https://docs.snowflake.com/en/user-guide/security-access-control-overview.html#label-access-control-role-enforcement) in use for the current session.",
	},
}

func CurrentRole() *schema.Resource {
	return &schema.Resource{
		Read:   ReadCurrentRole,
		Schema: currentRoleSchema,
	}
}

func ReadCurrentRole(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*sql.DB)
	role, err := snowflake.ReadCurrentRole(db)
	if err != nil {
		log.Printf("[DEBUG] current_role failed to decode")
		d.SetId("")
		return nil
	}

	d.SetId(fmt.Sprintf(role.Role))
	err = d.Set("name", role.Role)
	if err != nil {
		return err
	}
	return nil
}
