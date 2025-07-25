-- ***************************************************************
-- Feature Engineering for Customer Churn Dataset (PostgreSQL)
-- ***************************************************************

-- Step 1: Encode Categorical Variables to Numerical Formats
-- Converting categorical columns into numerical representations makes them usable for ML models.

-- 1.1 Encoding 'gender' as binary numeric (Male=1, Female=0)
ALTER TABLE customer_info.customer_churn
ADD COLUMN gender_sc INTEGER;

UPDATE customer_info.customer_churn
SET gender_sc = CASE WHEN gender = 'Male' THEN 1 ELSE 0 END;


-- 1.2 One-hot encoding 'InternetService' into three binary columns:
-- 'internet_dsl', 'internet_fiber', 'internet_no' to represent different internet service types
ALTER TABLE customer_info.customer_churn
ADD COLUMN internet_dsl INTEGER,
ADD COLUMN internet_fiber INTEGER,
ADD COLUMN internet_no INTEGER;

UPDATE customer_info.customer_churn
SET internet_dsl = CASE WHEN internetservice = 'DSL' THEN 1 ELSE 0 END,
    internet_fiber = CASE WHEN internetservice = 'Fiber optic' THEN 1 ELSE 0 END,
    internet_no = CASE WHEN internetservice = 'No' THEN 1 ELSE 0 END;


-- 1.3 Encoding 'Contract' types into ordinal numeric values:
-- Month-to-month=0, One year=1, Two year=2
ALTER TABLE customer_info.customer_churn
ADD COLUMN contract_numeric INTEGER;

UPDATE customer_info.customer_churn
SET contract_numeric = CASE
    WHEN Contract = 'Month-to-month' THEN 0
    WHEN Contract = 'One year' THEN 1
    WHEN Contract = 'Two year' THEN 2
    ELSE NULL
END;


-- Step 2: Aggregate Service Usage
-- Create 'num_services' by counting the number of 'Yes' responses across service-related features.
-- This feature summarizes overall customer engagement with multiple services.

ALTER TABLE customer_info.customer_churn
ADD COLUMN num_services INTEGER;

UPDATE customer_info.customer_churn
SET num_services =
    (CASE WHEN OnlineSecurity = 'Yes' THEN 1 ELSE 0 END
   + CASE WHEN OnlineBackup = 'Yes' THEN 1 ELSE 0 END
   + CASE WHEN DeviceProtection = 'Yes' THEN 1 ELSE 0 END
   + CASE WHEN TechSupport = 'Yes' THEN 1 ELSE 0 END
   + CASE WHEN StreamingTV = 'Yes' THEN 1 ELSE 0 END
   + CASE WHEN StreamingMovies = 'Yes' THEN 1 ELSE 0 END
    );


-- Step 3: Create Tenure Bins (Categorical Bucketing)
-- Transform 'tenure' numerical values into categorical bins: 'Short', 'Medium', 'Long'
-- Helps capture customer lifecycle stage for better model insight.

ALTER TABLE customer_info.customer_churn
ADD COLUMN Tenure_bin TEXT;

UPDATE customer_info.customer_churn
SET Tenure_bin = CASE
    WHEN tenure <= 12 THEN 'Short'
    WHEN tenure > 12 AND tenure <= 24 THEN 'Medium'
    WHEN tenure > 24 THEN 'Long'
    ELSE 'UNKNOWN'
END;


-- Step 4: One-Hot Encoding of Tenure Bins
-- Convert tenure bins to numeric binary columns for ML usage

ALTER TABLE customer_info.customer_churn
ADD COLUMN Tenure_bin_short INTEGER,
ADD COLUMN Tenure_bin_med INTEGER,
ADD COLUMN Tenure_bin_long INTEGER;

UPDATE customer_info.customer_churn
SET Tenure_bin_short = CASE WHEN tenure_bin = 'Short' THEN 1 ELSE 0 END,
    Tenure_bin_med = CASE WHEN tenure_bin = 'Medium' THEN 1 ELSE 0 END,
    Tenure_bin_long = CASE WHEN tenure_bin = 'Long' THEN 1 ELSE 0 END;


-- Step 5: Normalize 'MonthlyCharges' feature
-- Min-Max scaling applied to normalize values between 0 and 1
-- Normalization improves convergence for models sensitive to feature scale.

ALTER TABLE customer_info.customer_churn
ADD COLUMN monthlycharges_norm FLOAT;

UPDATE customer_info.customer_churn
SET monthlycharges_norm = 
    (monthlycharges - (SELECT MIN(monthlycharges) FROM customer_info.customer_churn)) /
    ((SELECT MAX(monthlycharges) FROM customer_info.customer_churn) - (SELECT MIN(monthlycharges) FROM customer_info.customer_churn));


-- Step 6: Create a View with Engineered Features for Easy Access
-- This view contains only relevant columns for modeling, combining both raw and engineered features.

CREATE VIEW customer_info.customer_churn_info AS
SELECT 
    customerid,
    seniorcitizen,
    internet_dsl,
    internet_fiber,
    internet_no,
    contract_numeric,
    num_services,
    Tenure_bin_short,
    Tenure_bin_med,
    Tenure_bin_long,
    monthlycharges_norm,
    gender_sc
FROM customer_info.customer_churn;


-- Step 7: Export Prepared Data to CSV for Use in Python or Other ML Tools
-- Export the view to a CSV file for seamless data integration into modeling pipelines.

COPY (
    SELECT * FROM customer_info.customer_churn_info
) TO 'C:\Work folder\Python for data analysis\customer_churn_data.csv' DELIMITER ',' CSV HEADER;


