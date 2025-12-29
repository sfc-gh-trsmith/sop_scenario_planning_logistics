-- =============================================================================
-- 06_cortex_search.sql
-- Cortex Search service setup for S&OP Scenario Planning & Logistics
-- 
-- Creates:
--   - Document chunks table for parsed PDF/DOCX content
--   - Cortex Search Service: SUPPLY_CHAIN_DOCS_SEARCH
--
-- Document types indexed:
--   - 3PL contracts
--   - Warehouse capacity SLAs
--   - S&OP meeting minutes
--
-- Usage: Run after documents are uploaded to RAW.DOCS_STAGE
-- =============================================================================

USE ROLE IDENTIFIER($PROJECT_ROLE);
USE DATABASE IDENTIFIER($FULL_PREFIX);
USE WAREHOUSE IDENTIFIER($PROJECT_WH);
USE SCHEMA SOP_LOGISTICS;

-- =============================================================================
-- Step 1: Create Document Metadata Table
-- Stores document-level metadata before chunking
-- Note: Use IF NOT EXISTS to preserve data during redeployment
-- =============================================================================
CREATE TABLE IF NOT EXISTS DOCUMENT_METADATA (
    DOCUMENT_ID NUMBER(38,0) AUTOINCREMENT,
    FILE_PATH VARCHAR(500) NOT NULL,
    FILE_NAME VARCHAR(200) NOT NULL,
    DOCUMENT_TYPE VARCHAR(50),  -- 'CONTRACT', 'SLA', 'MEETING_MINUTES'
    VENDOR_NAME VARCHAR(200),
    REGION VARCHAR(100),
    EFFECTIVE_DATE DATE,
    EXPIRY_DATE DATE,
    UPLOADED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    PRIMARY KEY (DOCUMENT_ID)
);

COMMENT ON TABLE DOCUMENT_METADATA IS 'Metadata for uploaded supply chain documents';

-- =============================================================================
-- Step 2: Create Document Chunks Table
-- Stores parsed and chunked text for Cortex Search indexing
-- Note: Use IF NOT EXISTS to preserve data during redeployment
-- =============================================================================
CREATE TABLE IF NOT EXISTS DOCUMENT_CHUNKS (
    CHUNK_ID NUMBER(38,0) AUTOINCREMENT,
    DOCUMENT_ID NUMBER(38,0) NOT NULL,
    
    -- Content
    CHUNK_TEXT VARCHAR(16000) NOT NULL,  -- Chunk content for search
    CHUNK_INDEX NUMBER(38,0) NOT NULL,   -- Order within document
    
    -- Metadata for filtering/display
    RELATIVE_PATH VARCHAR(500),
    FILE_URL VARCHAR(1000),
    DOCUMENT_TYPE VARCHAR(50),
    VENDOR_NAME VARCHAR(200),
    REGION VARCHAR(100),
    PAGE_NUMBER NUMBER(38,0),
    
    -- Audit
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    PRIMARY KEY (CHUNK_ID),
    FOREIGN KEY (DOCUMENT_ID) REFERENCES DOCUMENT_METADATA(DOCUMENT_ID)
);

COMMENT ON TABLE DOCUMENT_CHUNKS IS 'Parsed and chunked document text for Cortex Search indexing';
COMMENT ON COLUMN DOCUMENT_CHUNKS.CHUNK_TEXT IS 'Text chunk (target: ~500 tokens) for semantic search';

-- =============================================================================
-- Step 3: Create Stored Procedure to Parse and Chunk Documents
-- Uses PARSE_DOCUMENT function to extract text from PDFs
-- =============================================================================
CREATE OR REPLACE PROCEDURE PARSE_STAGED_DOCUMENTS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    files_processed INTEGER DEFAULT 0;
    chunks_created INTEGER DEFAULT 0;
    doc_id INTEGER;
BEGIN
    -- Process each PDF file in the stage
    FOR file_record IN (
        SELECT 
            relative_path,
            file_url,
            SPLIT_PART(relative_path, '/', -1) AS file_name
        FROM DIRECTORY(@RAW.DOCS_STAGE)
        WHERE LOWER(relative_path) LIKE '%.pdf'
           OR LOWER(relative_path) LIKE '%.docx'
    ) DO
        -- Insert document metadata
        INSERT INTO DOCUMENT_METADATA (FILE_PATH, FILE_NAME, DOCUMENT_TYPE, VENDOR_NAME, REGION)
        SELECT 
            file_record.relative_path,
            file_record.file_name,
            CASE 
                WHEN LOWER(file_record.file_name) LIKE '%contract%' THEN 'CONTRACT'
                WHEN LOWER(file_record.file_name) LIKE '%sla%' THEN 'SLA'
                WHEN LOWER(file_record.file_name) LIKE '%meeting%' OR LOWER(file_record.file_name) LIKE '%minutes%' THEN 'MEETING_MINUTES'
                ELSE 'OTHER'
            END,
            CASE 
                WHEN LOWER(file_record.file_name) LIKE '%acme%' THEN 'ACME Logistics'
                WHEN LOWER(file_record.file_name) LIKE '%northeast%' THEN 'Northeast 3PL'
                ELSE 'Unknown'
            END,
            CASE 
                WHEN LOWER(file_record.file_name) LIKE '%northeast%' THEN 'Northeast'
                WHEN LOWER(file_record.file_name) LIKE '%west%' THEN 'West'
                ELSE 'National'
            END;
        
        -- Get the document ID we just inserted
        SELECT MAX(DOCUMENT_ID) INTO :doc_id FROM DOCUMENT_METADATA;
        
        -- Parse document and create chunks using Snowflake Cortex PARSE_DOCUMENT
        INSERT INTO DOCUMENT_CHUNKS (
            DOCUMENT_ID, CHUNK_TEXT, CHUNK_INDEX, RELATIVE_PATH, FILE_URL,
            DOCUMENT_TYPE, VENDOR_NAME, REGION, PAGE_NUMBER
        )
        SELECT 
            :doc_id,
            SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
                @RAW.DOCS_STAGE, 
                file_record.relative_path,
                {'mode': 'LAYOUT'}
            ):content::VARCHAR,
            1,
            file_record.relative_path,
            file_record.file_url,
            dm.DOCUMENT_TYPE,
            dm.VENDOR_NAME,
            dm.REGION,
            1
        FROM DOCUMENT_METADATA dm
        WHERE dm.DOCUMENT_ID = :doc_id;
        
        files_processed := files_processed + 1;
    END FOR;
    
    -- Count total chunks created
    SELECT COUNT(*) INTO :chunks_created FROM DOCUMENT_CHUNKS;
    
    RETURN 'Processed ' || files_processed || ' files, created ' || chunks_created || ' chunks';
END;
$$;

COMMENT ON PROCEDURE PARSE_STAGED_DOCUMENTS() IS 'Parses PDF/DOCX files from DOCS_STAGE and creates searchable chunks';

-- =============================================================================
-- Step 4: Create Cortex Search Service
-- Service: SUPPLY_CHAIN_DOCS_SEARCH
-- =============================================================================
CREATE OR REPLACE CORTEX SEARCH SERVICE SUPPLY_CHAIN_DOCS_SEARCH
    ON CHUNK_TEXT
    ATTRIBUTES DOCUMENT_TYPE, VENDOR_NAME, REGION, RELATIVE_PATH
    WAREHOUSE = IDENTIFIER($PROJECT_WH)
    TARGET_LAG = '1 minute'
    COMMENT = 'Cortex Search service for supply chain documents - supports RAG queries'
AS (
    SELECT 
        CHUNK_ID,           -- Required: Agent uses this as id_column for result tracking
        CHUNK_TEXT,
        DOCUMENT_TYPE,
        VENDOR_NAME,
        REGION,
        RELATIVE_PATH,
        FILE_URL,
        PAGE_NUMBER
    FROM DOCUMENT_CHUNKS
);

-- =============================================================================
-- Step 5: Grant Access to Search Service
-- =============================================================================
GRANT USAGE ON CORTEX SEARCH SERVICE SUPPLY_CHAIN_DOCS_SEARCH TO ROLE IDENTIFIER($PROJECT_ROLE);

-- =============================================================================
-- Step 6: Create Helper View for Search Results
-- Enriches search results with full document metadata
-- =============================================================================
CREATE OR REPLACE VIEW SEARCH_RESULTS_V AS
SELECT
    dc.CHUNK_ID,
    dc.CHUNK_TEXT,
    dc.CHUNK_INDEX,
    dc.PAGE_NUMBER,
    dm.DOCUMENT_ID,
    dm.FILE_NAME,
    dm.FILE_PATH,
    dm.DOCUMENT_TYPE,
    dm.VENDOR_NAME,
    dm.REGION,
    dm.EFFECTIVE_DATE,
    dm.EXPIRY_DATE,
    dc.FILE_URL
FROM DOCUMENT_CHUNKS dc
JOIN DOCUMENT_METADATA dm ON dc.DOCUMENT_ID = dm.DOCUMENT_ID;

COMMENT ON VIEW SEARCH_RESULTS_V IS 'Enriched view of document chunks with full metadata';

-- =============================================================================
-- Verification
-- =============================================================================
SHOW CORTEX SEARCH SERVICES IN SCHEMA SOP_LOGISTICS;
DESCRIBE CORTEX SEARCH SERVICE SUPPLY_CHAIN_DOCS_SEARCH;

