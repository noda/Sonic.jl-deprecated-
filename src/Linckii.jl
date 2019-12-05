module Linckii

import Dates
import HTTP
import JSON
import JuliaDB
import Unitful

function get_access(secret)
    return secret
end

function get(access, query; kwargs...)
    response = HTTP.get(
        join(
            [
                "$(access.url)/api/v1/$(query)",
                ["&$(k)=$(v)" for (k, v) in kwargs]...
            ],
        );
        headers = Dict(
            "Authorization" => "Key $(access.key)",
        ),
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

function get_nodes(access; kwargs...)
    json = get(access, "node"; kwargs...)["nodes"]
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

function get_devices(access; kwargs...)
    json = get(access, "device"; kwargs...)["devices"]
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

function get_sensors(access; kwargs...)
    json = get(access, "sensor"; kwargs...)["sensors"]
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

function get_datetime(ts)
    return Dates.DateTime(ts[1 : 19])
end

function get_timezone(ts)
    t = Dates.Time(12)
    z = Dates.Time(ts[end - 4 : end]) - Dates.Time(0)
    return ts[end - 5] == '-' ? t - z : t + z # 12:00:00±HH:MM
end

function get_data(access, node_id, sensor_name, dates...; kwargs...)
    json = vcat(
        [
            get(
                access,
                "query/measurement?start=$(a1)&end=$(a2)";
                node_id = node_id, quantity = sensor_name, kwargs...
            )["data"]
            for (a1, a2) in zip(dates[1 : end - 1], dates[2 : end])
        ]...
    )
    t = JuliaDB.table(
        [
            (;
                :datetime => get_datetime(value["ts"]),
                :timezone => get_timezone(value["ts"]),
                :value => value["v"],
            )
            for d in json
            for value in d["values"]
        ],
        pkey = :datetime,
    )
    return t
end

function set_data(access, node_id, sensor_name, data; kwargs...)
    error("not implemented")
end

function sitepath(access)
    site_name = split(split(access.url, "://")[2], "/")[2]
    return joinpath("db", "linckii", site_name)
end

function savesite(access; kwargs...)
    path = sitepath(access)
    mkpath(sitepath(access))
    for (k, v) in kwargs
        JuliaDB.save(
            v,
            joinpath(path, "$(k).db"),
        )
    end
end

function loadsite(access, args...)
    path = sitepath(access)
    return (;
        (
            k => JuliaDB.load(
                joinpath(path, "$(k).db"),
            )
            for k in args
        )...
    )
end

function datapath(access, node_id)
    return joinpath(sitepath(access), "data", "$(node_id)")
end

function datapath(access, node_id, sensor_name)
    return joinpath(datapath(access, node_id), String(sensor_name))
end

function savedata(access, node_id, sensor_name, dates...; kwargs...)
    data = get_data(access, node_id, sensor_name, dates...; kwargs...)
    mkpath(datapath(access, node_id))
    JuliaDB.save(
        data,
        "$(datapath(access, node_id, sensor_name)).db",
    )
end

function loaddata(access, node_id, sensor_name)
    return JuliaDB.load(
        "$(datapath(access, node_id, sensor_name)).db",
    )
end

end
