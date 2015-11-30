-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

--[[
Redshift output for filtered / sanitized FirefoxOS First Time Use (ftu) data.

This inserts records "one to one" with incoming pings. To extract the aggregate
counts:

-- Optionally:
-- CREATE TABLE fxos_ftu AS SELECT * FROM fxos_ftu_incoming LIMIT 0;

INSERT INTO
    fxos_ftu
SELECT
    ping_date, submission_date, os, country, device, locale, update_channel,
    update_channel_standardized, platform_version, platform_build_id, sim_mcc,
    sim_mnc, network_mcc, network_mnc, screen_width, screen_height,
    device_pixel_ratio, software, hardware, firmware_revision, activation_date,
    sum(count)
FROM
    fxos_ftu_incoming
WHERE
    submission_date = X
GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21;

DELETE FROM fxos_ftu_incoming WHERE submission_date = X;

-- SELECT
    a.*,
    c.name AS country_name,
    c.continent,
    c.region,
    --l.name AS language_name,
    smc.country as sim_mcc_country,
    smo.name as sim_mnc_network,
    nmc.country as network_mcc_country,
    nmo.name as network_mnc_network

   FROM
    fxos_ftu a LEFT JOIN
    country c ON (a.country = c.code) LEFT JOIN
    --language l ON (a.language = l.code) LEFT JOIN
    mobile_code smc ON (a.sim_mcc = smc.code) LEFT JOIN
    mobile_code nmc ON (a.network_mcc = nmc.code) LEFT JOIN
    mobile_operator smo ON (a.sim_mcc = smo.mobile_code AND a.sim_mnc = smo.operator_code) LEFT JOIN
    mobile_operator nmo ON (a.network_mcc = nmo.mobile_code AND a.network_mnc = nmo.operator_code)

SELECT
    a.submission_date,
    c.name AS country_name,
    c.continent,
    c.region,
    --l.name AS language_name,
    smc.country as sim_mcc_country,
    smo.name as sim_mnc_network,
    nmc.country as network_mcc_country,
    nmo.name as network_mnc_network

FROM
    fxos_ftu a LEFT JOIN
    country c ON (a.country = c.code) LEFT JOIN
    --language l ON (a.language = l.code) LEFT JOIN
    mobile_code smc ON (a.sim_mcc = smc.code) LEFT JOIN
    mobile_code nmc ON (a.network_mcc = nmc.code) LEFT JOIN
    mobile_operator smo ON (a.sim_mcc = smo.mobile_code AND a.sim_mnc = smo.operator_code) LEFT JOIN
    mobile_operator nmo ON (a.network_mcc = nmo.mobile_code AND a.network_mnc = nmo.operator_code)
LIMIT 20;

Config:

- db_name (string)
- db_user (string)
- db_password (string)
- db_host (string)
- db_port (uint)
- buffer_file (string)
    Path to the buffer file
- buffer_size (uint, optional, default 10240000)
    File size, in bytes, of table inserts to buffer before bulk loading.
- flush_interval (uint, optional, default 60)
    The maximum amount of time in seconds between bulk load operations.  This
    should match the plugin ticker_interval configuration.

*Example Heka Configuration*

.. code-block:: ini

    [FxOSFTUOutput]
    type = "SandboxOutput"
    filename = "fxos_ftu.lua"
    message_matcher = "Type == 'telemetry' && Fields[appName] == 'FirefoxOS' && Fields[reason] == 'ftu'"
    memory_limit = 60000000
    ticker_interval = 60
    timer_event_on_shutdown = true

        [FxOSFTUOutput.config]
        db_name = "dev"
        db_user = "testuser"
        db_password = "testuserpw"
        db_host = "foobar.redshift.amazonaws.com"
        db_port = 5439
        buffer_file = "/var/tmp/fxos_ftu.insert"
        flush_interval = 60
--]]

local ds = require "derived_stream"
require "cjson"
require "io"
require "os"
require "string"
require "table"

-- Temporary record for collecting calculated fields.
record = {}
local table_name = "fxos_ftu_incoming"
local schema = {
--  column name                     type            length  attributes  field /function
    {"ping_date"                   ,"DATE"         ,nil    ,nil        ,function() return record["ping_date"] end},
    {"submission_date"             ,"DATE"         ,nil    ,"SORTKEY DISTKEY" ,"Fields[submissionDate]"},
    {"os"                          ,"VARCHAR"      ,100    ,nil        ,"Fields[os]"},
    {"country"                     ,"VARCHAR"      ,2      ,nil        ,"Fields[geoCountry]"},
    {"device"                      ,"VARCHAR"      ,200    ,nil        ,"Fields[device]"},
    {"locale"                      ,"VARCHAR"      ,10     ,nil        ,function() return record["locale"] end},
    {"language"                    ,"VARCHAR"      ,2      ,nil        ,function() return record["language"] end},
    {"update_channel"              ,"VARCHAR"      ,30     ,nil        ,"Fields[appUpdateChannel]"},
    {"update_channel_standardized" ,"VARCHAR"      ,30     ,nil        ,"Fields[normalizedChannel]"},
    {"platform_version"            ,"VARCHAR"      ,30     ,nil        ,"Fields[appVersion]"},
    {"platform_build_id"           ,"VARCHAR"      ,30     ,nil        ,"Fields[appBuildId]"},
    {"sim_mcc"                     ,"VARCHAR"      ,10     ,nil        ,function() return record["sim_mcc"] end},
    {"sim_mnc"                     ,"VARCHAR"      ,10     ,nil        ,function() return record["sim_mnc"] end},
    {"sim_network_name"            ,"VARCHAR"      ,30     ,nil        ,function() return record["sim_network_name"] end},
    {"network_mcc"                 ,"VARCHAR"      ,10     ,nil        ,function() return record["network_mcc"] end},
    {"network_mnc"                 ,"VARCHAR"      ,10     ,nil        ,function() return record["network_mnc"] end},
    {"network_network_name"        ,"VARCHAR"      ,30     ,nil        ,function() return record["network_network_name"] end},
    {"screen_width"                ,"INTEGER"      ,nil    ,nil        ,function() return record["screen_width"] end},
    {"screen_height"               ,"INTEGER"      ,nil    ,nil        ,function() return record["screen_height"] end},
    {"device_pixel_ratio"          ,"INTEGER"      ,nil    ,nil        ,function() return record["device_pixel_ratio"] end},
    {"software"                    ,"VARCHAR"      ,100    ,nil        ,function() return record["software"] end},
    {"hardware"                    ,"VARCHAR"      ,100    ,nil        ,function() return record["hardware"] end},
    {"firmware_revision"           ,"VARCHAR"      ,100    ,nil        ,function() return record["firmware_revision"] end},
    {"activation_date"             ,"DATE"         ,nil    ,nil        ,function() return record["activation_date"] end},
    {"count"                       ,"INTEGER"      ,nil    ,nil        ,function() return record["count"] end},
}

function sanity_check(payload)
    -- Check that the payloads have correct 'reason' ('ftu') and 'appName'
    -- ('FirefoxOS'), and check that the other 'info' fields are consistent with
    -- those prefixed with 'deviceinfo'.
    local info = payload["info"]
    if type(info) ~= "table" then
        return false
    end

    if info["appName"] ~= "FirefoxOS" or info["reason"] ~= "ftu" then
        return false
    end

    if type(payload["deviceinfo.update_channel"]) == "string" then
        if payload["deviceinfo.update_channel"] ~= info["appUpdateChannel"] then
            return false
        end
    end

    if type(payload["deviceinfo.platform_version"]) == "string" then
        if payload["deviceinfo.platform_version"] ~= info["appVersion"] then
            return false
        end
    end

    if type(payload["deviceinfo.platform_build_id"]) == "string" then
        if payload["deviceinfo.platform_build_id"] ~= info["appBuildID"] then
            return false
        end
    end

    return true
end

local function millis_to_date(millis)
    if type(millis) ~= "number" then
        return nil
    end
    if millis <= 0 then
        return nil
    end
    return os.date("%Y-%m-%d", millis / 1e3)
end

local function format_tarako(r)
    if not r.device then
        return
    end

    for i, pfx in ipairs({'Intex', 'Spice', 'Ace', 'Zen'}) do
        if string.sub(r.device, 1, string.len(pfx)) == pfx then
            r['os'] = '1.3T'
            break
        end
    end
end

local function format_os(o)
-- os_subs = [
--     {
--         'regex': re.compile('[.\-]prerelease$', re.I),
--         'repl': ' (pre-release)'
--     },{
--         'regex': re.compile(
--             '^(?P<num>[1-9]\.[0-9](\.[1-9]){0,2})(\.0){0,2}', re.I),
--         'repl': '\g<num>'
--     },{
--         # For now, Tarako label is based on mapping from partner/device.
--         'regex': re.compile('^(ind|intex)_.+$', re.I),
--         'repl': '1.3T'
--     }
-- ]
    -- TODO apply all
    return o
end

local function format_device(d)
-- device_subs = [
--     {
--         # One Touch Fire.
--         'regex': re.compile(
--             '^.*one\s*touch.*fire\s*(?P<suffix>[ce]?)(?:\s+\S*)?$', re.I),
--         'repl': lambda match: add_suffix('One Touch Fire',
--             match.group('suffix').upper())
--     },{
--        # Open 2/C.
--         'regex': re.compile(
--             '^.*open\s*(?P<suffix>[2c])(?:\\s+\\S*)?$', re.I),
--         'repl': lambda match: 'ZTE Open ' + match.group('suffix').upper()
--     },{
--         # Open.
--         'regex': re.compile('^.*open\s*$', re.I),
--         'repl': 'ZTE Open'
--     },{
--         # Flame.
--         'regex': re.compile('^.*flame.*$', re.I),
--         'repl': 'Flame'
--     },{
--         # Geeksphone.
--         'regex': re.compile('^.*(keon|peak|revolution).*$', re.I),
--         # 'repl': lambda match: 'Geeksphone ' + match.group(1).capitalize()
--         'repl': 'Geeksphone'
--     },{
--         # Emulators/dev devices
--         'regex': re.compile('^.*(android|aosp).*$', re.I),
--         'repl': 'Emulator/Android'
--     },{
--         # Tarako - Cloud FX.
--         'regex': re.compile('^.*clou.?d\\s*fx.*$', re.I),
--         'repl': 'Intex Cloud FX'
--     },{
--         # Tarako - Spice.
--         'regex': re.compile('^.*spice(\\s*|_)mi-?fx(?P<ver>[12]).*$', re.I),
--         'repl': lambda match: 'Spice MIFX' + match.group('ver')
--         # 'repl': 'Spice MIFX1'
--     },{
--         # Tarako - Cherry Ace.
--         'regex': re.compile('^ace\\s*f100.*$', re.I),
--         'repl': 'Ace F100'
--     },{
--         # Fire C device in Peru
--         'regex': re.compile('^4019a$', re.I),
--         'repl': 'One Touch Fire C'
--     },{
--         # Zen U105.
--         'regex': re.compile('^.*u105.*$', re.I),
--         'repl': 'Zen U105'
--     },{
--         # Fx0.
--         'regex': re.compile('^lgl25.*$', re.I),
--         'repl': 'Fx0'
--     },{
--         # Pixi 3.
--         'regex': re.compile('^.*pixi\\s*3(\\s+\\(?|\\()3\\.5\\)?.*$', re.I),
--         'repl': 'Pixi 3 (3.5)'
--     },{
--         # Orange Klif.
--         'regex': re.compile('^.*klif.*$', re.I),
--         'repl': 'Orange Klif'
--     },{
--         # Panasonic TV.
--         'regex': re.compile('^ptv-.*$', re.I),
--         'repl': 'Panasonic TV'
--     },{
--         # Sony Xperia Z3C.
--         'regex': re.compile('^.*xperia\s*z3\s*c(ompact)?(\W+.*)?$', re.I),
--         'repl': 'Xperia Z3C'
--     }
--     # ,{
--         # # GoFox.
--         # 'regex': re.compile('^.*gofox.*$', re.I),
--         # 'repl': 'GoFox F15'
--     # }
-- ]
    -- TODO apply one
    return d
end

-- These prefixes map to themselves
local nn_prefixes = {
  'A1', 'Aircel', 'Airtel', 'AIS', 'Alltel', 'AT&T', 'B-Mobile', 'Banglalink',
  'Base', 'Batelco', 'Bell', 'Bite', 'blau', 'Bob', 'Bouygues', 'Breeze', 'CCT',
  'Cellular One', 'Claro', 'Cloud9', 'Congstar', 'Corr', 'CTBC', 'delight',
  'Digicel', 'Digitel', 'Digital', 'disco', 'Djuice', 'DNA', 'Dolphin', 'DTAC',
  'E-Plus', 'Econet', 'eMobile', 'Emtel', 'Entel', 'Etisalat', 'Euskatel',
  'Farmers', 'Fastweb', 'Fonex', 'Free', 'Gemalto', 'Globalstar', 'Globe',
  'GLOBUL', 'Golan', 'Golden Telecom', 'Hello', 'Highland', 'Hits', 'Hormuud',
  'HT', 'ICE', 'Idea', 'Indigo', 'Indosat', 'Jawwal', 'Jazztel', 'KTF',
  'Libertis', 'Maroc Telecom', 'MIO', 'Mobilis', 'mobilR', 'mobily', 'Mobistar',
  'Moov', 'Movilnet', 'Namaste', 'Nawras', 'NEP', 'Netz', 'Nextel', 'Nitz',
  'O2', 'olleh', 'One.Tel', 'OnePhone', 'Orange', 'Outremer', 'OY', 'Play',
  'Plus', 'Poka Lambro', 'Polska Telefonia', 'Reliance', 'Robi', 'Rogers',
  'Rwandatel', 'Scarlet', 'SERCOM', 'SFR', 'Simyo', 'SingTel', 'SKT',
  'SmarTone', 'Smile', 'Softbank', 'Southern Communications', 'Spacetel',
  'Tango', 'Telcel', 'Telenor', 'Teletalk', 'Tele.ring', 'Telma', 'Telstra',
  'Telus', 'Tesco', 'Test', 'Thinta', 'Thuraya', 'Tigo', 'TMA', 'True',
  'Tuenti', 'Unicom', 'Uninor', 'UTS', 'Vectone', 'Velcom', 'Videocon',
  'Viettel', 'VIP', 'Virgin', 'Viva', 'Vivo', 'VoiceStream', 'VTR', 'Warid',
  'Wataniya', 'Wind', 'XL', 'Yesss', 'Yoigo', 'Zain'
}

-- These prefixes map to something else
local nn_prefix_map = {
  {'!dea',              'Idea'},
  {'AKTel',             'Robi'},
  {'Comcel',            'Claro'},
  {'Grameen',           'Grameenphone'},
  {'GP',                'Grameenphone'},
  {'Liaoning',          'China Mobile'},
  {'TATA Teleservices', 'Docomo'},
  {'celcom',            'Cellcom'},
  {'esto es el',        'Unknown'},
  {'lyca',              'Lyca Mobile'},
  {'mudio',             'Mundio'},
  {'voda',              'Vodafone'}
}

local nn_patterns = {
  {'3[^a-zA-Z0-9_].+$',                    '3'},
  {'bee%s*line(%s.+)?$',                   'Beeline'},
  {'bh%s*mobile(%s.+)?$',                  'BH Mobile'},
  {'(.+%s)?bsnl(%s.+)?$',                  'BSNL'},
  {'cab(le|el) (&|and) wireless.*$',       'Cable & Wireless'},
  {'(chn-)?(unicom|cu[^a-zA-Z0-9_]*(cc|gsm)).*$',  'China Unicom'},
  {'CMCC$',                                'China Mobile'},
  {'(chungh?wa.*|CHT)$',                   'Chunghwa'},
  {'.*cingular.*$',                        'Cingular'},
  {'(.+%s)?cosmote(%s.+)?$',               'Cosmote'},
  {'da?tatel(%s.+)?$',                     'Datatel'},
  {'diall?og$',                            'Dialog'},
  {'digi([^a-zA-Z0-9_]+.*)?$',             'Digi'},
  {'(.+%s)?docomo(%s.+)?$',                'Docomo'},
  {'glo(%s.+)?$',                          'Glo'},
  {'gramee?n(phone)?$',                    'Grameenphone'},
  {'guin.tel.*$',                          'Guinetel'},
  {'life(%s.+)?$',                         'life:)'},
  {'lime(%s.+)?$',                         'Lime'},
  {'m[:-]?tel(%s.+)?$',                    'M-Tel'},
  {'medion%s*mobile(%s.+)?',               'Medion'},
  {'mobil?com([^a-zA-Z0-9_].+)?$',         'Mobilcom'},
  {'mobil?tel(%s.+)?$',                    'Mobitel'},
  {'(.+%s)?movie?star(%s.+)?$',            'Movistar'},
  {'oi(%s.+)?$',                           'Oi'},
  {'proxi(mus)?(%s.+)?$',                  'Proximus'},
  {'Sask%s?[Ttel].*$',                     'SaskTel'},
  {'smarts?(%s.+)?$',                      'Smart'},
  {'s%s+tel.*$',                           'S Tel'},
  {'sun(%s.+)?$',                          'Sun'},
  {'t%s*-%s*mobile.*$',                    'T-Mobile'},
  {'.*tele?%s*2.*$',                       'Tele2'},
  {'tel[a-zA-Z0-9_]+%scel$',               'Telecel'},
  {'telekom\.de(%s.+)?$',                  'T-Mobile'},
  {'telekom(\.|%s)hu(%s.+)?$',             'T-Mobile'},
  {'tm([^a-zA-Z0-9_].+)?$',                'TM'},
  {'tw%s*m(obile)?(%s.+)?$',               'Taiwan Mobile'},
  {'.*verizon.*$',                         'Verizon'},
  {'vid.otron.*$',                         'Videotron'},
  -- redundant {'vip([^a-zA-Z0-9_].*)?$',                       'VIP'},
  {'W1(%s.+)?$',                           'WirelessOne'},
  {'Wikes Cellular$',                      'Wilkes Cellular'},
}

-- Case-insensitive prefix match
local function prefix_imatch(full, pre)
    if not full or type(full) ~= "string" then
        return false
    end

    return string.lower(pre) == string.lower(string.sub(full, 1, string.len(pre)))
end

local function format_network_name(n)
    if not n or type(n) ~= "string" then
        return nil
    end

    -- Return the prefix directly
    for i, prefix in ipairs(nn_prefixes) do
        if prefix_imatch(n, prefix) then
            return prefix
        end
    end

    -- Return the thing the prefix maps to
    for i, pm in ipairs(nn_prefix_map) do
        if prefix_imatch(n, pm[1]) then
            return pm[2]
        end
    end

    -- Apply patterns
    for i, pt in ipairs(nn_patterns) do
        a, b = string.find(string.lower(n), pt[1])
        if a == 1 then
            return pt[2]
        end
    end

    -- Complicated patterns:
    -- TODO
    -- {'^(?:.+\s)?china.*\s(?P<suffix>mobile|telecom|unicom)(\s.+)?$', lambda match: 'China ' + match.group('suffix').capitalize()},
    -- {'^mt:?(?P<suffix>[cns])([^\w].*)?$', lambda match: 'MT' + match.group('suffix').upper()},

    -- TODO
    -- # More general pattern matching (eg. spelling differences).
    --
    -- TODO: return first match
    return n
end

local function is_empty(v)
    return v == nil or v == ''
end

local function format_ftu(p)
    local r = {}

    -- Start with message fields
    for i, col in ipairs(schema) do
        if type(col[5]) == "string" then
            r[col[1]] = read_message(col[5])
        end
    end

    -- p.info is a table after earlier sanity_check(p)
    r.ping_date          = millis_to_date(p.info.pingTime)
    r.device             = p.info["deviceinfo.product_model"]
    r.locale             = p.info["locale"]
    if string.len(r.locale) >= 2 then
        r.language = string.sub(r.locale, 1, 2)
    end

    if type(p.info.icc) == "table" then
        r.sim_mcc        = p.info.icc["mcc"]
        r.sim_mnc        = p.info.icc["mnc"]
        r.sim_network_name = format_network_name(p.info.icc["spn"])
    end
    if type(p.info.network) == "table" then
        r.network_mcc    = p.info.network["mcc"]
        r.network_mnc    = p.info.network["mnc"]
        r.network_network_name = format_network_name(p.info.network["operator"])
    end
    r.screen_width       = p.info["screenWidth"]
    r.screen_height      = p.info["screenHeight"]
    r.device_pixel_ratio = p.info["devicePixelRatio"]
    r.software           = p.info["deviceinfo.software"]
    r.hardware           = p.info["deviceinfo.hardware"]
    r.firmware_revision  = p.info["deviceinfo.firmware_revision"]
    r.activation_date    = millis_to_date(p.info.activationTime)
    r.count = 1

    -- Merge update channel fields.
    if is_empty(r.update_channel) and not is_empty(p.info["app.update.channel"]) then
        r.update_channel = p.info["app.update.channel"]
    end
    format_tarako(r)

    r.os = format_os(r.os)
    r.device = format_device(r.device)
    -- TODO: general formatting

    return r
end

--------------------------

local ds_pm
ds_pm, timer_event = ds.load_schema(table_name, schema)

function process_message()
    -- Parse the JSON payload
    local ok, parsed = pcall(cjson.decode, read_message("Payload"))
    if not ok or not sanity_check(parsed) then return -1 end

    record, err = format_ftu(parsed)
    -- TODO: graph errors by type, or insert them into an errors table?
    if err then return -1 end

    return ds_pm()
end
