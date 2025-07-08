# Customer 360: The Customer's Choice

**Customer 360** is a modern, cloud-based retail analytics platform designed to streamline online shopping experiences. It integrates customer behavior, transaction patterns, and sales performance into a unified system. With interactive dashboards and personalized insights, it empowers businesses to make data-driven decisions and enhance customer satisfaction.

---

## Problem Statement

Customer 360 is focused on delivering a unified, data-driven understanding of customer behavior to support strategic business decisions. However, critical customer-related data—such as profile details, support ticket information, transaction records, and web interaction logs—are fragmented across various systems.

This project aims to build a robust and scalable data pipeline that integrates all relevant customer data into a centralized **Snowflake** data warehouse. By consolidating diverse datasets into a single source of truth, Customer 360 eliminates data silos and ensures consistency across analytical outputs.

Data transformation processes are carried out using **SQL** and **PySpark**, including data cleansing, normalization, and enrichment. The curated data is visualized using **Power BI dashboards**, offering insights into customer behavior, issue resolution patterns, and engagement metrics.

With this end-to-end architecture, Customer 360 can deliver accurate analytics, enhance customer experiences, and enable data-driven decision-making across the organization.

---

##  Technologies Used

| Technology                | Purpose                                                                 |
|---------------------------|-------------------------------------------------------------------------|
| **Azure Blob Storage**    | Initial storage for raw `.xlsx` files                                   |
| **Azure Data Lake (ADLS)**| Intermediate Delta storage optimized for analytics                      |
| **Azure Databricks**      | Data ingestion and transformation using PySpark                         |
| **Snowflake**             | Centralized data warehouse with Bronze–Silver–Gold schema architecture  |
| **Power BI**              | Real-time dashboards and business insights                              |

---

## Software Requirements

- Microsoft Azure Subscription
- Snowflake Account
- Power BI Account
- Azure Databricks Workspace

---

## Data Sources

The following `.xlsx` files serve as input to the pipeline. All files are initially stored in **Azure Blob Storage** and later transformed in the pipeline.

### 1. `Customer_master.xlsx`
**Description**: Contains customer personal and identifiable information  
**Fields**:
- `customer_id`: Unique identifier
- `name`: Full name
- `email`: Email address
- `phone`: Contact number
- `signup_date`: Date of registration
- `location`: Geographical location

---

### 2. `Customer_support.xlsx`
**Description**: Logs support ticket details raised by customers  
**Fields**:
- `ticket_id`: Unique ticket ID
- `customer_id`: Reference to customer
- `created_at`: Ticket creation time
- `resolved_at`: Resolution time
- `issue_type`: Nature of issue
- `satisfaction_score`: Rating (1–5)

---

### 3. `Transaction.xlsx`
**Description**: Details of customer transactions  
**Fields**:
- `transaction_id`: Transaction ID
- `customer_id`: Reference to customer
- `transaction_date`: Date of transaction
- `amount`: Transaction amount
- `payment_method`: Mode (e.g., UPI, credit card)
- `store_location`: Store or service location

---

### 4. `Web_Interactions.xlsx`
**Description**: Captures browsing and session data  
**Fields**:
- `session_id`: Session ID
- `customer_id`: Reference to customer
- `page_views`: Total pages viewed
- `session_duration`: Duration (seconds/minutes)
- `device`: Device used
- `interaction_date`: Date of session

---

