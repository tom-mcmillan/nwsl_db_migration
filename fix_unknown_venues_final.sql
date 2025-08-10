-- Fix Unknown Venues in NWSL Database - Final Version
-- This script maps unknown venues to their correct venue entries based on address matching

BEGIN;

-- Create a temporary mapping table with the preferred venue for each address
CREATE TEMP TABLE venue_mapping AS
WITH ranked_venues AS (
    SELECT 
        address,
        id,
        name,
        -- Prioritize venues with capacity data and newer stadium names
        ROW_NUMBER() OVER (
            PARTITION BY address 
            ORDER BY 
                CASE WHEN capacity IS NOT NULL THEN 0 ELSE 1 END,
                CASE 
                    -- Prefer current stadium names
                    WHEN name LIKE '%BMO Stadium%' THEN 1
                    WHEN name LIKE '%CPKC Stadium%' THEN 1
                    WHEN name LIKE '%Providence Park%' THEN 1
                    WHEN name LIKE '%Lynn Family Stadium%' THEN 1
                    WHEN name LIKE '%Shell Energy Stadium%' THEN 1
                    WHEN name LIKE '%Lumen Field%' THEN 1
                    WHEN name LIKE '%Snapdragon Stadium%' THEN 1
                    WHEN name LIKE '%PayPal Park%' THEN 1
                    WHEN name LIKE '%Inter&Co Stadium%' THEN 1
                    WHEN name LIKE '%America First Field%' THEN 1
                    WHEN name LIKE '%Audi Field%' THEN 1
                    WHEN name LIKE '%Red Bull Arena%' THEN 1
                    WHEN name LIKE '%Exploria Stadium%' THEN 1
                    WHEN name LIKE '%First Horizon Stadium%' THEN 2
                    WHEN name LIKE '%Children''s Mercy Park%' THEN 2
                    ELSE 3
                END,
                LENGTH(name) DESC  -- Prefer longer, more descriptive names
        ) as rn
    FROM venue
    WHERE address IS NOT NULL
    AND name NOT LIKE 'Unidentified Venue%'
)
SELECT address, id as correct_venue_id, name as correct_venue_name
FROM ranked_venues
WHERE rn = 1;

-- Show before state
SELECT 'BEFORE: Unknown venues count' as status, COUNT(*) as count
FROM venue WHERE name LIKE 'Unidentified Venue%';

-- Update match_venue_weather to point to correct venues
UPDATE match_venue_weather mvw
SET venue_uuid = vm.correct_venue_id
FROM venue uv, venue_mapping vm
WHERE mvw.venue_uuid = uv.id
AND uv.name LIKE 'Unidentified Venue%'
AND mvw.venue_location = vm.address;

SELECT 'Updated match_venue_weather records' as status, COUNT(*) as count
FROM match_venue_weather mvw
JOIN venue_mapping vm ON mvw.venue_location = vm.address
WHERE mvw.venue_uuid = vm.correct_venue_id;

-- Delete the now-unused unknown venue records
DELETE FROM venue
WHERE name LIKE 'Unidentified Venue%'
AND id NOT IN (SELECT DISTINCT venue_uuid FROM match_venue_weather WHERE venue_uuid IS NOT NULL);

-- Show after state
SELECT 'AFTER: Unknown venues count' as status, COUNT(*) as count
FROM venue WHERE name LIKE 'Unidentified Venue%';

SELECT 'AFTER: Total venues count' as status, COUNT(*) as count
FROM venue;

COMMIT;