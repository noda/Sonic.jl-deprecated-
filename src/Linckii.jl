module Linckii

import Dates
import HTTP
import JSON
import JuliaDB
import Unitful

include("LinckiiSecrets.jl")

function get(query, site_name; kwargs ...)
    k = LinckiiSecrets.key[site_name]
    h = Dict("Authorization" => "Key $(k)")
    response = HTTP.get(
        join(
            [
                "https://canary.noda.se/~$(site_name)/api/v1/$(query)",
                ["&$(k)=$(v)" for (k, v) in kwargs] ...
            ],
        );
        headers = h,
    )
    return JSON.parse(String(response.body))
end

function table(dicts; kwargs...)
    return JuliaDB.table(
        [
            (; (Symbol(k) => v for (k, v) in dict) ...)
            for dict in dicts
        ];
        kwargs...
    )
end

function get_nodes(site_name; sensor_id = true, kwargs ...)
    json = get("node", site_name; kwargs ...)["nodes"]
    t = table(json, pkey = :id)
    t = JuliaDB.rename(
        t,
        n => Symbol("node_$(n)")
        for n in JuliaDB.colnames(t)
    )
    t = JuliaDB.rename(
        t,
        :node_device => :device_id,
        :node_sensor_ids => :sensor_ids,
    )
    if sensor_id
        t = JuliaDB.table(
            [
                (;
                    delete!(Dict(pairs(r)), :sensor_ids) ...,
                    :sensor_id => sensor_id,
                )
                for r in JuliaDB.rows(t)
                for sensor_id in Set(r.sensor_ids)
            ],
        )
    end
    return t
end

function get_devices(site_name; kwargs ...)
    json = get("device", site_name; kwargs ...)["devices"]
    t = table(json, pkey = :id)
    t = JuliaDB.rename(
        t,
        n => Symbol("device_$(n)")
        for n in JuliaDB.colnames(t)
    )
    t = JuliaDB.rename(
        t,
        :device_protocol_id => :protocol_id,
    )
    return t
end

sensor_units = Dict(
    ""          => Unitful.nothing,
    "&deg;"     => Unitful.nothing,
    "°"         => Unitful.nothing,
    "bit"       => Unitful.nothing,
    "bool"      => Unitful.nothing,
    "int"       => Unitful.nothing,
    "%"         => Unitful.percent,
    "&deg;C"    => Unitful.°C,
    "°C"        => Unitful.°C,
    "&deg;h"    => Unitful.K * Unitful.h,
    "bar"       => Unitful.bar,
    "m3"        => Unitful.m^3,
    "m³"        => Unitful.m^3,
    "kW"        => 10^3 * Unitful.W,
    "kWh"       => 10^3 * Unitful.W * Unitful.h,
    "MWh"       => 10^6 * Unitful.W * Unitful.h,
    "l/h"       => Unitful.l / Unitful.h,
    "m/s"       => Unitful.m / Unitful.s,
    "Sec"       => Unitful.s,
)

function get_sensors(site_name; kwargs ...)
    json = get("sensor", site_name; kwargs ...)["sensors"]
    t = table(json, pkey = :id)
    t = JuliaDB.rename(
        t,
        n => Symbol("sensor_$(n)")
        for n in JuliaDB.colnames(t)
    )
    t = JuliaDB.rename(
        t,
        :sensor_postfix => :sensor_unit,
        :sensor_protocol_id => :protocol_id,
    )
    t = JuliaDB.transform(
        t,
        :sensor_unit => :sensor_unit => s -> sensor_units[s],
    )
    return t
end

function split_ts(ts)
    return (Dates.DateTime(ts[1 : 19]), Dates.Time(ts[end - 4 : end]))
end

function get_measurements(site, args ...; kwargs ...)
    jsons = [
        get("query/measurement?start=$(a1)&end=$(a2)", site; kwargs ...)["data"]
        for (a1, a2) in zip(args[1 : end - 1], args[2 : end])
    ]
    json = vcat(jsons...)
    return JuliaDB.transform(
        table(
            [
                merge!(Dict(k => v for (k, v) in dict if k != "values"), value)
                for dict in json
                    for value in dict["values"]
            ],
        ),
        :ts => :ts => split_ts,
    )
end

function get_site_structure(site_name)
    t = get_nodes(site_name)
    t = JuliaDB.join(
        t,
        get_devices(site_name);
        lkey = :device_id,
        rkey = :device_id,
    )
    t = JuliaDB.join(
        t,
        get_sensors(site_name);
        lkey = :sensor_id,
        rkey = :sensor_id,
    )
    return t
end

end
