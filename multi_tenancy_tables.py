import postgresql
import psycopg2
from psycopg2.errors import DuplicateColumn

query = '''
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema IN ('public', 'awd', 'brand_analytics', 'business_reports', 'fulfillment_inbound', 'inventory', 'listings_items', 'product_pricing', 'products', 'rankings', 'sponsored_brands', 'sponsored_display', 'sponsored_products', 'orders')
                AND table_name NOT IN ('tenants', 'amazon_marketplaces')
                AND TABLE_TYPE = 'BASE TABLE';
        '''
        
with postgresql.setup_cursor() as cur:
    cur.execute(query)
    tables = cur.fetchall()

for table in tables:
    print(table['table_schema'], table['table_name'])
    schema_table = f"{table['table_schema']}.{table['table_name']}"

    with postgresql.setup_cursor() as cur:
        try:
            # Step 1: Add the tenant_id column with a default value of 1
            print(f'Adding tenant id to {schema_table}')
            cur.execute(f"""ALTER TABLE {schema_table}
                            ADD COLUMN tenant_id INT NOT NULL DEFAULT 1;""")

            # Step 2: Add a foreign key constraint to reference the tenants table
            print(f'\tAdding foreign key constraint. . .')
            cur.execute(f"""
                            ALTER TABLE {schema_table}
                            ADD CONSTRAINT fk_tenant_id
                            FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id);
                        """)
        except DuplicateColumn as error:
            print(error)