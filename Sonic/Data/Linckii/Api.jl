import HTTP
import JSON

import Secrets

function get(site, query)
    h = Dict("Authorization" => "Authorization: Key " + Secrets.key[site])
    return JSON.parse(String(HTTP.get("https://canary.noda.se/~$site/api/v1/$query"; headers = h)))
end

function get_nodes(site)
    return get(site, "node")
end

function get_sensors(site)
    return get(site, "sensor")
end

function get_devices(site)
    return get(site, "device")
end

function get_measurements(site, dates)
    l = length(dates)
    jsons = [
        get(site, "query/measurement?start=$date1&end=$date2")["data"]
        for (date1, date2) in zip(dates[1 : l - 1], dates[2 : l])
    ]
    return vcat(jsons...)
end
