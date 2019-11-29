module Linckii

import Dates
import HTTP
import JSON
import JuliaDB # https://github.com/JuliaComputing/JuliaDB.jl/issues/292
import Unitful

include("LinckiiSecrets.jl")

function get(query, site_name; kwargs...)
    k = LinckiiSecrets.key[site_name]
    h = Dict("Authorization" => "Key $(k)")
    response = HTTP.get(
        join(
            [
                "https://canary.noda.se/~$(site_name)/api/v1/$(query)",
                ["&$(k)=$(v)" for (k, v) in kwargs]...
            ],
        );
        headers = h,
    )
    return JSON.parse(String(response.body))
end

function json_table(json; kwargs...)
    return JuliaDB.table(
        [
            (; (Symbol(k) => v for (k, v) in d)...)
            for d in json
        ];
        kwargs...
    )
end

function get_nodes(site_name; kwargs...)
    json = get("node", site_name; kwargs...)["nodes"]
    t = json_table(json, pkey = :id)
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
    return t
end

function flatten_nodes(nodes)
    t = JuliaDB.flatten(nodes, :sensor_ids)
    t = JuliaDB.rename(
        t,
        :sensor_ids => :sensor_id,
    )
    t = JuliaDB.reindex(
        t,
        (:node_id, :sensor_id),
    )
    return t
end

function get_devices(site_name; kwargs...)
    json = get("device", site_name; kwargs...)["devices"]
    t = json_table(json, pkey = :id)
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
    "&deg;h"    => Unitful.hr * Unitful.K,
    "&deg;"     => Unitful.NoUnits,
    "MWh"       => Unitful.hr * Unitful.MW,
    "l/h"       => Unitful.l * Unitful.hr^-1,
    "Sec"       => Unitful.s,
    "bar"       => Unitful.bar,
    "bool"      => Unitful.NoUnits,
    "bit"       => Unitful.NoUnits,
    "int"       => Unitful.NoUnits,
    "m/s"       => Unitful.m * Unitful.s^-1,
    "m³"        => Unitful.m^3,
    "%"         => Unitful.percent,
    "&deg;C"    => Unitful.°C,
    "°C"        => Unitful.°C,
    "m3"        => Unitful.m^3,
    "°"         => Unitful.NoUnits,
    "kW"        => Unitful.kW,
    "kWh"       => Unitful.hr * Unitful.kW,
    ""          => Unitful.NoUnits,
)

function get_sensors(site_name; kwargs...)
    json = get("sensor", site_name; kwargs...)["sensors"]
    t = json_table(json, pkey = :id)
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
        :sensor_name => :sensor_name => s -> Symbol(s),
        :sensor_unit => :sensor_unit => s -> sensor_units[s],
    )
    return t
end

function get_datetime(timestamp)
    dt = Dates.DateTime(timestamp[1 : 19])
    return dt
end

function get_timezone(timestamp)
    tz = Dates.CompoundPeriod(
        Dates.Hour(timestamp[end - 4 : end - 3]),
        Dates.Minute(timestamp[end - 1 : end]),
    )
    tz = timestamp[end - 5] == '-' ? - tz : tz
    return tz
end

function get_data(site_name, pkeys, dates...; kwargs...)
    json = vcat(
        [
            get(
                "query/measurement?start=$(a1)&end=$(a2)", site_name;
                node_id = node_id, quantity = sensor_name, kwargs...
            )["data"]
            for (node_id, sensor_name) in pkeys
            for (a1, a2) in zip(dates[1 : end - 1], dates[2 : end])
        ]...
    )
    t = JuliaDB.table(
        [
            (;
                :node_id => d["facility"],
                :sensor_name => Symbol(d["quantity"]),
                :datetime => get_datetime(value["ts"]),
                :timezone => get_timezone(value["ts"]),
                :value => value["v"],
            )
            for d in json
            for value in d["values"]
        ],
        pkey = (:node_id, :sensor_name, :datetime),
    )
    return t
end

end
