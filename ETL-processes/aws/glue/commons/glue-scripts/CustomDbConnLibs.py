# ===============================================================================
# --- POSTGRES JDBC URL HELPER ---
# ===============================================================================
def get_postgres_jdbc_conf(glueContext, connection_name):

    """
    **Extracts credentials & URL from AWS Glue Connection Catalog.
    Relies on the Connection URL to specify target database, 
    eliminating need for a hardcoded DB name or an extra job parameter.
    """

    print(f"### -Extracting credentials from Glue Connection '{connection_name}'...")
    jdbc_conf = glueContext.extract_jdbc_conf(connection_name)
    
    connection_properties = {
        "driver": "org.postgresql.Driver"
    }
    
    if 'user' in jdbc_conf and jdbc_conf['user']:
        connection_properties['user'] = jdbc_conf['user']
    if 'password' in jdbc_conf and jdbc_conf['password']:
        connection_properties['password'] = jdbc_conf['password']
        
    jdbc_url = jdbc_conf.get('fullUrl') # FIX: Prioritize 'fullUrl' w/c contains DB name, fallback to 'url'
    if not jdbc_url:
        jdbc_url = jdbc_conf.get('url', '')
    
    print(f"### -Extracted jdbc_url string: '{jdbc_url}'")
    
    return jdbc_url, connection_properties

# ===============================================================================
# --- POSTGRES RAW QUERY HELPER ---
# ===============================================================================
def execute_postgres_query(glueContext, jdbc_url, properties, query):
    """Executes a raw SQL statement (like DELETE) using Spark's native JVM driver"""
    connection = None
    try:
        driver = glueContext.spark_session._sc._gateway.jvm.java.sql.DriverManager
        java_props = glueContext.spark_session._sc._gateway.jvm.java.util.Properties()
        java_props.setProperty("user", properties.get("user", ""))
        java_props.setProperty("password", properties.get("password", ""))
        java_props.setProperty("driver", properties.get("driver", "org.postgresql.Driver"))
        
        connection = driver.getConnection(jdbc_url, java_props)
        statement = connection.createStatement()
        statement.executeUpdate(query)
        print(f"### -SUCCESS: Executed raw query: {query}")
    except Exception as e:
        raise Exception(f"Failed to execute raw query: {str(e)}")
    finally:
        if connection is not None:
            connection.close()