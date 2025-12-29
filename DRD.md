# **Demo Requirements Document (DRD): S\&OP Integrated Scenario Planning & Logistics Optimization**

## **1\. Strategic Overview**

* **Problem Statement:** Manufacturing organizations struggle to bridge the gap between aggressive Sales/Marketing demand spikes (e.g., Q4 pushes) and rigid Production capacity. This misalignment often results in disjointed spreadsheets, unaccounted logistics/warehousing costs for "pre-build" inventory, and a slow, manual S\&OP approval process that lacks data lineage and version control.  
* **Target Business Goals (KPIs):**  
  * **Optimize Net Working Capital:** Balance pre-build inventory holding costs against revenue upside.  
  * **Increase Order Fill Rate:** Ensure Q4 demand satisfaction by locking in Q3 production schedules.  
  * **Reduce Planning Cycle Time:** Move from weeks of spreadsheet iterations to real-time scenario agreement.  
* **The "Wow" Moment:** The VP of Operations toggles between "Baseline Forecast" and "Marketing Q4 Push" versions in Streamlit. The dashboard instantly recalculates the required Q3 production utilization and the resulting spike in Warehousing Costs (retrieved via Snowflake Hybrid Tables). The user then asks Cortex Analyst, *"What is the margin impact of the increased storage costs in Version 2?"* and receives an instant SQL-generated answer.

## **2\. User Personas & Stories**

| Persona Level | Role Title | Key User Story (Demo Flow) |
| :---- | :---- | :---- |
| **Strategic** | **VP of Supply Chain** | "As a VP, I want to compare the 'Marketing Push' plan against the 'Baseline' plan to see if the revenue uplift justifies the extra warehousing and logistics costs required for the pre-build." |
| **Operational** | **Demand/Sales Planner** | "As a Planner, I want to upload a new forecast version ('Plan V2') into the system without overwriting the official record, so I can demonstrate the need for a Q3 pre-build strategy." |
| **Technical** | **Logistics Analyst** | "As an Analyst, I want to use ML to forecast the warehouse capacity utilization based on the incoming production schedule to prevent overflow penalties." |

## **3\. Data Architecture & Snowpark ML (Backend)**

### **Structured Data (Inferred Schema)**

* **\[DEMAND\_FORECAST\_VERSIONS\] (Hybrid Table):** Stores forecast data enabling low-latency write-back for versioning.  
  * *Columns:* SKU\_ID, FISCAL\_PERIOD, QUANTITY, SCENARIO\_ID (e.g., 'Base', 'Q4\_Push'), VERSION\_TIMESTAMP.  
* **\[PRODUCTION\_CAPACITY\]:** Static and dynamic constraints.  
  * *Columns:* PLANT\_ID, LINE\_ID, MAX\_UNITS\_PER\_SHIFT, AVAILABLE\_SHIFTS.  
* **\[LOGISTICS\_COST\_FACT\]:** Financial impact data.  
  * *Columns:* WAREHOUSE\_ID, STORAGE\_COST\_PER\_PALLET, TRANSPORT\_COST\_UNIT, OVERFLOW\_PENALTY\_RATE.

### **Unstructured Data (Tribal Knowledge)**

* **Source Material:** Third-Party Logistics (3PL) Contracts, Warehouse Capacity SLAs, and S\&OP Meeting Minutes (PDF/Docx).  
* **Purpose:** Used to answer qualitative questions via Cortex Search, such as identifying contractual limits on overflow storage during the pre-build period.

### **ML Notebook Specification (Snowpark ML)**

* **Objective:** **Pre-build Inventory Optimization & Feasibility Check**.  
* **Target Variable:** OPTIMAL\_PRODUCTION\_RATE (for Q3).  
* **Algorithm Choice:** Linear Programming (Optimization) or XGBoost (to predict lead time variability/bottlenecks).  
* **Inference Output:** A suggested production schedule written to table \[RECOMMENDED\_BUILD\_PLAN\] that smoothes the curve across Q3 to meet Q4 demand without exceeding max warehouse capacity.

## **4\. Cortex Intelligence Specifications**

### **Cortex Analyst (Structured Data / SQL)**

* **Semantic Model Scope:**  
  * **Measures:** Total Forecasted Revenue, Projected Warehousing Cost, Capacity Utilization %, Inventory Units On-Hand.  
  * **Dimensions:** Scenario Name (Base vs. Q4 Push), Product Family, Region, Fiscal Month.  
* **Golden Query (Verification):**  
  * **User Prompt:** "Compare the total warehousing cost between the Baseline and Q4 Push scenarios for October."  
  * **Expected SQL Operation:**  
    SQL  
    SELECT scenario\_id, SUM(projected\_warehousing\_cost)  
    FROM logistics\_cost\_fact  
    WHERE fiscal\_month \= 'October'  
    GROUP BY scenario\_id;

### **Cortex Search (Unstructured Data / RAG)**

* **Service Name:** SUPPLY\_CHAIN\_DOCS\_SEARCH  
* **Indexing Strategy:**  
  * **Document Attribute:** Index by Contract\_Type (e.g., 'Warehousing', 'Logistics') and Vendor\_Name.  
* **Sample RAG Prompt:** "What are the contractual penalty fees if we exceed our storage capacity by more than 10% in the Northeast distribution center?"

## **5\. Streamlit Application UX/UI**

### **Layout Strategy**

* Page 1 (S\&OP Executive Dashboard):  
  \*  
  * Split view comparing "Plan A (Base)" vs "Plan B (Pre-build)".  
  * KPI cards highlighting the variance in **Revenue** vs. **Cost of Carry** (Warehousing/Logistics).  
* **Page 2 (Scenario Builder & Write-Back):**  
  * Editable Dataframe (Snowflake Hybrid Tables) where Sales/Marketing inputs the "Q4 Push" numbers.  
  * "Submit Version" button that triggers a Snowpark Stored Procedure to calculate the downstream supply chain impact.

### **Component Logic**

* **Visualizations:** Altair Area Chart showing the "Inventory Build-up" curve (The "Camel Hump" showing inventory rising in Q3 and depleting in Q4).  
* **Chat Integration:** A sidebar Cortex Agent where the user can ask:  
  1. *Quantitative:* "How much inventory do we need to produce in September?" (Routes to Analyst).  
  2. *Qualitative:* "Do we have approval logic in the meeting minutes for this type of spend?" (Routes to Search).

## **6\. Success Criteria**

* **Technical Validator:** The solution successfully captures a user input (New Forecast Version) via Streamlit, writes it to a Hybrid Table, and updates the aggregate visualizations in under 5 seconds using Dynamic Tables.  
* **Business Validator:** The workflow enables the S\&OP team to identify that the "Q4 Push" requires a 20% increase in warehousing budget, allowing them to approve the logistics spend *before* the production orders are released.