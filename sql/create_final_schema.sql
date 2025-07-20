-- Drop tables in dependency order
DROP TABLE IF EXISTS rehab_details;
DROP TABLE IF EXISTS rehab_estimates;
DROP TABLE IF EXISTS valuation_details;
DROP TABLE IF EXISTS hoa_details;
DROP TABLE IF EXISTS taxes;
DROP TABLE IF EXISTS leads;
DROP TABLE IF EXISTS properties;

-- Properties table (main entity with all property-specific fields)
CREATE TABLE properties (
    property_id INT AUTO_INCREMENT PRIMARY KEY,
    property_title VARCHAR(500),
    address VARCHAR(500),
    street_address VARCHAR(300),
    city VARCHAR(100) NOT NULL,
    state VARCHAR(50) NOT NULL,
    zip VARCHAR(20),
    property_type VARCHAR(100),
    market VARCHAR(100),
    year_built INT,

    -- Property characteristics
    flood VARCHAR(50),
    highway VARCHAR(50),
    train VARCHAR(50),
    tax_rate DECIMAL(8,4),
    sqft_basement INT,
    htw VARCHAR(50),
    pool VARCHAR(50),
    commercial VARCHAR(50),
    water VARCHAR(100),
    sewage VARCHAR(100),
    sqft_mu INT,
    sqft_total INT,
    parking VARCHAR(100),
    bed INT,
    bath INT,
    basement_yes_no VARCHAR(10),
    layout VARCHAR(100),

    -- Location and ratings
    neighborhood_rating INT,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    subdivision VARCHAR(200),
    school_average DECIMAL(5,2),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_city_state (city, state),
    INDEX idx_property_type (property_type),
    INDEX idx_market (market),
    INDEX idx_zip (zip)
);

-- Leads table (lead management and investment metrics)
CREATE TABLE leads (
    lead_id INT AUTO_INCREMENT PRIMARY KEY,
    property_id INT NOT NULL,
    reviewed_status VARCHAR(100),
    most_recent_status VARCHAR(100),
    source VARCHAR(100),
    occupancy VARCHAR(100),
    net_yield DECIMAL(8,4),
    irr DECIMAL(8,4),
    selling_reason TEXT,
    seller_retained_broker VARCHAR(200),
    final_reviewer VARCHAR(200),
    rent_restricted VARCHAR(50),

    FOREIGN KEY (property_id) REFERENCES properties(property_id) ON DELETE CASCADE,
    INDEX idx_reviewed_status (reviewed_status),
    INDEX idx_source (source)
);

-- Taxes table (separate tax information)
CREATE TABLE taxes (
    tax_id INT AUTO_INCREMENT PRIMARY KEY,
    property_id INT NOT NULL,
    taxes DECIMAL(12,2),
    tax_year YEAR,

    FOREIGN KEY (property_id) REFERENCES properties(property_id) ON DELETE CASCADE
);

-- HOA Details table (multiple HOA records per property)
CREATE TABLE hoa_details (
    hoa_detail_id INT AUTO_INCREMENT PRIMARY KEY,
    property_id INT NOT NULL,
    hoa_fee DECIMAL(10,2),
    hoa_flag VARCHAR(10),
    sequence_number INT DEFAULT 1,

    FOREIGN KEY (property_id) REFERENCES properties(property_id) ON DELETE CASCADE,
    INDEX idx_hoa_flag (hoa_flag),
    UNIQUE KEY unique_property_sequence (property_id, sequence_number)
);

-- Valuation Details table (multiple valuations per property)
CREATE TABLE valuation_details (
    valuation_detail_id INT AUTO_INCREMENT PRIMARY KEY,
    property_id INT NOT NULL,
    sequence_number INT DEFAULT 1,

    -- All possible valuation fields
    previous_rent DECIMAL(10,2),
    list_price DECIMAL(15,2),
    zestimate DECIMAL(15,2),
    arv DECIMAL(15,2),
    expected_rent DECIMAL(10,2),
    rent_zestimate DECIMAL(10,2),
    low_fmr DECIMAL(10,2),
    high_fmr DECIMAL(10,2),
    redfin_value DECIMAL(15,2),

    valuation_date DATE DEFAULT (CURRENT_DATE),

    FOREIGN KEY (property_id) REFERENCES properties(property_id) ON DELETE CASCADE,
    INDEX idx_list_price (list_price),
    INDEX idx_arv (arv),
    UNIQUE KEY unique_property_val_sequence (property_id, sequence_number)
);

-- Rehab Estimates table (multiple rehab estimates per property)
CREATE TABLE rehab_estimates (
    rehab_estimate_id INT AUTO_INCREMENT PRIMARY KEY,
    property_id INT NOT NULL,
    sequence_number INT DEFAULT 1,

    underwriting_rehab DECIMAL(12,2),
    rehab_calculation DECIMAL(12,2),

    FOREIGN KEY (property_id) REFERENCES properties(property_id) ON DELETE CASCADE,
    INDEX idx_underwriting_rehab (underwriting_rehab),
    UNIQUE KEY unique_property_rehab_sequence (property_id, sequence_number)
);

-- Rehab Details table (detailed rehab breakdown for each estimate)
CREATE TABLE rehab_details (
    rehab_detail_id INT AUTO_INCREMENT PRIMARY KEY,
    rehab_estimate_id INT NOT NULL,

    paint VARCHAR(10),
    flooring_flag VARCHAR(10),
    foundation_flag VARCHAR(10),
    roof_flag VARCHAR(10),
    hvac_flag VARCHAR(10),
    kitchen_flag VARCHAR(10),
    bathroom_flag VARCHAR(10),
    appliances_flag VARCHAR(10),
    windows_flag VARCHAR(10),
    landscaping_flag VARCHAR(10),
    trashout_flag VARCHAR(10),

    FOREIGN KEY (rehab_estimate_id) REFERENCES rehab_estimates(rehab_estimate_id) ON DELETE CASCADE
);
