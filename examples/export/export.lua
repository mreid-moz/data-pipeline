-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

require "cjson"

function process_message()
    local msg = {
        Timestamp = read_message("Timestamp") / 1e9,
        Type = read_message("Type"),
        Hostname = read_message("Hostname"),
    }

    -- TODO: reconstitute the full payload (minus the pieces irrelevant to
    --       the data comparison).

    while true do
        local value_type, name, value, representation, count = read_next_field()
        if not name then break end

        -- Skip some telemetry-specific stuff.
        -- TODO: clarify which (if any) pieces we can omit here.
        if --name ~= "payload.addonDetails" and
           --name ~= "payload.addonHistograms" and
           --name ~= "payload.childPayloads" and
           name ~= "payload.chromeHangs" and
           --name ~= "payload.fileIOReports" and
           name ~= "payload.histograms" and
           --name ~= "payload.info" and
           --name ~= "payload.keyedHistograms" and
           --name ~= "payload.lateWrites" and
           --name ~= "payload.log" and
           --name ~= "payload.simpleMeasurements" and
           --name ~= "payload.slowSQL" and
           --name ~= "payload.slowSQLstartup" and
           --name ~= "payload.threadHangStats" and
           name ~= "payload.UIMeasurements" then
            -- Keep the first occurence only
            if not msg[name] then
                msg[name] = value
            end
        end
    end

    if msg.clientId == nil then
        return -1, "missing clientId"
    end

    local arr = "[" .. cjson.encode(msg) .. "," .. read_message("Payload") .. "]"
    inject_payload("txt", "output", msg.clientId .. "\t" .. arr .. "\n")
    return 0
end
