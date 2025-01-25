-- Marketplace Table
CREATE TABLE amazon_marketplaces (
    marketplace_name VARCHAR(2) PRIMARY KEY,
    endpoint TEXT NOT NULL,
    marketplace_id VARCHAR(50) NOT NULL,
    region VARCHAR(50) NOT NULL
);

-- https://developer-docs.amazon.com/sp-api/docs/marketplace-ids
-- From SP-API Marketplaces class
INSERT INTO amazon_marketplaces (marketplace_name, endpoint, marketplace_id, region)
VALUES
    ('AE', 'https://sellingpartnerapi-eu.amazon.com', 'A2VIGQ35RCS4UG', 'eu-west-1'),
    ('BE', 'https://sellingpartnerapi-eu.amazon.com', 'AMEN7PMS3EDWL', 'eu-west-1'),
    ('DE', 'https://sellingpartnerapi-eu.amazon.com', 'A1PA6795UKMFR9', 'eu-west-1'),
    ('PL', 'https://sellingpartnerapi-eu.amazon.com', 'A1C3SOZRARQ6R3', 'eu-west-1'),
    ('EG', 'https://sellingpartnerapi-eu.amazon.com', 'ARBP9OOSHTCHU', 'eu-west-1'),
    ('ES', 'https://sellingpartnerapi-eu.amazon.com', 'A1RKKUPIHCS9HS', 'eu-west-1'),
    ('FR', 'https://sellingpartnerapi-eu.amazon.com', 'A13V1IB3VIYZZH', 'eu-west-1'),
-- Same as UK
--('GB', 'https://sellingpartnerapi-eu.amazon.com', 'A1F83G8C2ARO7P', 'eu-west-1'),
    ('IN', 'https://sellingpartnerapi-eu.amazon.com', 'A21TJRUUN4KGV', 'eu-west-1'),
    ('IT', 'https://sellingpartnerapi-eu.amazon.com', 'APJ6JRA9NG5V4', 'eu-west-1'),
    ('NL', 'https://sellingpartnerapi-eu.amazon.com', 'A1805IZSGTT6HS', 'eu-west-1'),
    ('SA', 'https://sellingpartnerapi-eu.amazon.com', 'A17E79C6D8DWNP', 'eu-west-1'),
    ('SE', 'https://sellingpartnerapi-eu.amazon.com', 'A2NODRKZP88ZB9', 'eu-west-1'),
    ('TR', 'https://sellingpartnerapi-eu.amazon.com', 'A33AVAJ2PDY3EV', 'eu-west-1'),
    ('UK', 'https://sellingpartnerapi-eu.amazon.com', 'A1F83G8C2ARO7P', 'eu-west-1'),
    ('ZA', 'https://sellingpartnerapi-eu.amazon.com', 'AE08WJ6YKNBMC', 'eu-west-1'),
    ('AU', 'https://sellingpartnerapi-fe.amazon.com', 'A39IBJ37TRP1C6', 'us-west-2'),
    ('JP', 'https://sellingpartnerapi-fe.amazon.com', 'A1VC38T7YXB528', 'us-west-2'),
    ('SG', 'https://sellingpartnerapi-fe.amazon.com', 'A19VAU5U5O7RUS', 'us-west-2'),
    ('US', 'https://sellingpartnerapi-na.amazon.com', 'ATVPDKIKX0DER', 'us-east-1'),
    ('BR', 'https://sellingpartnerapi-na.amazon.com', 'A2Q3Y263D00KWC', 'us-east-1'),
    ('CA', 'https://sellingpartnerapi-na.amazon.com', 'A2EUQ1WTGCTBG2', 'us-east-1'),
    ('MX', 'https://sellingpartnerapi-na.amazon.com', 'A1AM78C64UM0Y8', 'us-east-1');