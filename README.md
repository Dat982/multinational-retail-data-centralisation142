# Multinational Retail Data Centralisation Project
In this initiative, we’re creating a robust, local <b>PostgreSQL database</b> to consolidate and harmonize sales data from various sources. The goal is to establish a <b>single source of truth</b> for sales information within our multinational company.

## Technologies Used
<ul><li>PostgreSQL: Our trusty database engine.</li>
<li>AWS (S3): For efficient data storage.</li>
<li>boto3: Interact with AWS services programmatically.</li>
<li>REST APIs: Facilitate data retrieval.</li>
<li>CSV: Handle data in tabular format.</li>
<li>Python (Pandas): Process and manipulate data.</li></ul>

## Database Credentials Setup
As part of data management process, the following YAML files exist in the project’s root directory that contain specific credentials to establish connections:

<ol><li>db_creds_local.yaml: This file contains the necessary credentials for connecting to the local PostgreSQL database.</li>
<li>db_creds.yaml: Here, you’ll find the credentials required to access the AWS RDS database, which holds the source data.</li></ol>
Both files include the following keys:

<ul><li>RDS_HOST: The hostname or IP address of the database server.</li>
<li>RDS_PASSWORD: The secure password associated with your database user.</li>
<li>RDS_USER: Your authorized database username.</li>
<li>RDS_DATABASE: The name of the database schema.</li>
<li>RDS_PORT: The port number for communication.</li></ul>
These credentials are essential for seamless data retrieval and analysis. 

## ETL Pipeline
1.  **Data Extraction from Multiple Sources:**
    -   Extract data from diverse sources, including APIs, JSON files, PDFs, RDS databases, and S3 buckets.
    -   Load each dataset into a Pandas DataFrame.
    -   **Specific Data Sources:**
        -   User details and order information are sourced from a PostgreSQL database hosted on AWS RDS. These datasets are loaded into the following tables:
            -   **sales_data.dim_users**
            -   **sales_data.orders_table**
        -   Credit card details are extracted from PDF documents and loaded into the **sales_data.dim_card_details** table.
        -   Store details are obtained via an API and loaded into the **sales_data.dim_store_details** table.
        -   Product details are sourced from an S3 bucket and loaded into the **sales_data.dim_products** table.
        -   Date events are extracted from JSON files and loaded into the **sales_data.dim_date_times** table.
2.  **Database Schema Design:**
    -   Develop a star-based schema for the PostgreSQL database.
    -   Ensure that columns have appropriate data types.
    -   Utilize SQL (executed via SQLAlchemy) to perform the following tasks:
        -   Update data types for selected columns.
        -   Derive additional columns using CASE/WHEN statements (e.g., creating a weight class column based on weight).
        -   Define primary keys and establish foreign key relationships.
3.  **Data Analysis and Reporting:**

-   Utilize SQL queries to extract meaningful insights from the PostgreSQL database.
-   Generate a comprehensive report of business metrics by addressing the following questions:
    -   **Store Locations:**
        -   Identify locations with the highest number of stores.
    -   **Sales Trends:**
        -   Determine which months exhibit the highest sales.
    -   **Store Contribution:**
        -   Calculate the percentage of sales contributed by each store type.
    -   **Sales Velocity:**
        -   Analyze the average time interval between consecutive sales, grouped by year.

This systematic approach ensures efficient data management and empowers informed decision-making within the organization.
