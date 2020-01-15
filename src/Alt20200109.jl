module LinckiiAPIConfigs

keab = (;
    dataname => "keab",
    dataroot => "data",
    endpoint => "https://canary.noda.se/~keab/api/v1",
    passname => "SECRET",
)

end

module LinckiiAPI

import Dates
import HTTP
import JSON
import Unitful

function get_access(config)
    return (;
        config...,
    )
end

function HTTP_get(access, resource; kwargs...)
    r = HTTP.get(
        "$(access.endpoint)/$(resource)",
        ["Authorization" => "Key $(access.passname)"];
        kwargs...,
    )
    return String(r.body)
end

function metapath(config)
    return joinpath(
        config.dataroot,
        config.dataname,
        "meta.api",
    )
end

function savemeta(access, name, resource)
    p = metapath(access)
    mkpath(p)
    q = joinpath(p, "$(name).json")
    try
        s = HTTP_get(access, resource)
        open(q; write = true) do f
            write(f, s)
        end
        println("$(q): save")
    catch e
        println("$(q): $(e)")
    end
end

function savemeta(access)
    for (n, r) in (
        "d" => "device",
        "n" => "node",
        "s" => "sensor",
    )
        savemeta(access, n, r)
    end
end

function loadmeta(config, name)
    p = metapath(config)
    q = joinpath(p, "$(name).json")
    c = nothing
    try
        open(q) do f
            c = read(f)
        end
        println("$(q): load")
    catch e
        println("$(q): $(e)")
    end
    return JSON.parse(String(c))
end

function mk_row(json, re_n, re_k, re_v)
    return (;
        (
            k => get(re_v, k, identity)(v)
            for (k, v) in (
                get(re_k, k, k) => v
                for (k, v) in (
                    re_n(k) => v
                    for (k, v) in json
                )
            )
        )...,
    )
end

function mkdrow(json)
    return mk_row(
        json,
        s -> Symbol("device_$(s)"),
        Dict(
            :device_protocol_id => :protocol_id,
        ),
        Dict(
            :device_name => s -> Symbol(s),
        ),
    )
end

function mkdrows(json)
    return map(mkdrow, json["devices"])
end

function loaddrows(config)
    return mkdrows(loadmeta(config, "d"))
end

function mknrow(json)
    return mk_row(
        json,
        s -> Symbol("node_$(s)"),
        Dict(
            :node_device => :device_id,
            :node_sensor_ids => :sensor_ids,
        ),
        Dict(
            :node_name => s -> Symbol(s),
            :sensor_ids => sensor_ids -> map(Int64, sensor_ids),
        ),
    )
end

function mknrows(json)
    return map(mknrow, json["nodes"])
end

function loadnrows(config)
    return mknrows(loadmeta(config, "n"))
end

function loadnrows(config, patterns)
    d = Dict(
        r.sensor_id => r.sensor_name
        for r in loadsrows(config)
    )
    return filter(
        r -> (
            r.device_id in keys(patterns) &&
            Set(d[sensor_id] for sensor_id in r.sensor_ids) >=
            keys(patterns[r.device_id])
        ),
        loadnrows(access),
    )
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

function mksrow(json)
    return mk_row(
        json,
        s -> Symbol("sensor_$(s)"),
        Dict(
            :sensor_postfix => :sensor_unit,
            :sensor_protocol_id => :protocol_id,
        ),
        Dict(
            :sensor_name => s -> Symbol(s),
            :sensor_unit => s -> sensor_units[s],
        ),
    )
end

function mksrows(json)
    return map(mksrow, json["sensors"])
end

function loadsrows(config)
    return mksrows(loadmeta(config, "s"))
end

function datapath(config)
    return joinpath(
        config.dataroot,
        config.dataname,
        "data.api",
    )
end

function datapath(config, node_id)
    return joinpath(
        datapath(config),
        string(node_id),
    )
end

function datapath(config, node_id, sensor_name)
    return joinpath(
        datapath(config, node_id),
        string(sensor_name),
    )
end

function savedata(access, node_id, sensor_name, dates)
    p = datapath(access, node_id, sensor_name)
    mkpath(p)
    for (date1, date2) in zip(dates[1 : end - 1], dates[2 : end])
        q = joinpath(p, "$(date1)-$(date2).json")
        if !isfile(q)
            try
                query = [
                    "start" => string(date1),
                    "end" => string(date2),
                    "node_id" => node_id,
                    "quantity" => sensor_name,
                ]
                c = HTTP_get(access,"query/measurement"; query = query)
                open(q; write = true) do f
                    write(f, c)
                end
                println("$(q): save")
            catch e
                println("$(q): $(e)")
            end
        end
    end
end

function savedata(access, patterns, dates)
    for r in loadnrows(access, patterns)
        for sensor_name in keys(patterns[r.device_id])
            savedata(access, r.node_id, sensor_name, dates)
        end
    end
end

function datetime(timestamp)
    return Dates.DateTime(timestamp[1 : 19])
end

"""
    timezone(timestamp)

The timezone and the implisit DST are important for modelling social behaviours,
but it would be better to have them as a separate signal rather than  as part of
the timestamp. Or reconstruct from elsewhere.

As a workaround for datetime-timezone arithmetics, encode the timezone offset by
12:00:00±HH:MM.
"""
function timezone(timestamp)
    t = Dates.Time(12)
    z = Dates.Time(timestamp[end - 4 : end]) - Dates.Time(0)
    return timestamp[end - 5] == '-' ? t - z : t + z
end

function mkxrow(json)
    return (;
        :datetime => datetime(json["ts"]),
        :value => Float64(json["v"]),
    )
end

function mkxrows(json)
    return collect(
        Iterators.flatten(
            (
                map(mkxrow, d["values"])
                for d in json
            ),
        ),
    )
end

function loaddata(config, node_id, sensor_name)
    p = datapath(config, node_id, sensor_name)
    xrs = []
    for n in filter(n -> endswith(n, ".json"), readdir(p))
        q = joinpath(p, n)
        try
            c = nothing
            open(q) do f
                c = read(f)
            end
            append!(xrs, mkxrows(JSON.parse(String(c))["data"]))
            println("$(q): load")
        catch e
            println("$(q): $(e)")
        end
    end
    return xrs
end

function loaddata(config, node_id)
    p = datapath(config, node_id)
    return Dict(
        sensor_name => loaddata(config, node_id, sensor_name)
        for sensor_name in map(Symbol, readdir(p))
    )
end

function loaddata(config)
    p = datapath(config)
    return Dict(
        node_id => loaddata(config, node_id)
        for node_id in map(s -> parse(Int64, s), readdir(p))
    )
end

end

module Linckii

import DataFrames
import Dates

import LinckiiAPI
import Nodosus

function metapath(config)
    return joinpath(
        config.dataroot,
        config.dataname,
        "meta",
    )
end

function savemeta(config)
    m = LinckiiAPI.loadmeta(config)
    error("Not Implemented")
end

function loadmeta(config)
    error("Not Implemented")
end

function datapath(config)
    return joinpath(
        config.dataroot,
        config.dataname,
        "data",
    )
end

function savedata(config, patterns)
    p = datapath(config)
    for r in LinckiiAPI.loadnrows(config, patterns)
        q = joinpath(p, "$(r.node_id).csv")
        try
            df = DataFrames.DataFrame(
                (
                    (;
                        :variable => sensor_name,
                        r...,
                    )
                    for r in pp(LinckiiAPI.loaddata(config, r.node_id, sensor_name))
                    for (sensor_name, pp) in patterns[r.device_id]
                ),
            )
            df = DataFrames.unstack(df)

            DataFrames.DataFrame(:sensor_name => )
            # merge into one table
            # save
            c = ...
            open(q; write = true) do f
                write(f, c)
            end
            println("$(q): save")
        catch e
            println("$(q): $(e)")
        end
    end
end

function loaddata(config, node_id)
    p = datapath(config)
    q = joinpath(p, "$(r.node_id).csv")
    c = nothing
    try
        open(q) do f
            c = read(f)
        end
        println("$(q): load")
    catch e
        println("$(q): $(e)")
    end
    return c # should convert to dataframe or something
end

function loaddata(config)
    p = datapath(config)
    return Dict(
        node_id => loaddata(config, node_id)
        for node_id in map(s -> parse(Int64, s), readdir(p))
    )
end

end

module EvoAPIConfigs

veab = (;
    :dataname => "veab",
    :dataroot => "data",
    :endpoint => "https://evo.elvaco.se/api/v1",
    :password => "SECRET",
    :username => "jens.brage@noda.se",
)

end

module EvoAPI

import HTTP
import JSON
import Unitful

function get_access(config)
    r = HTTP.get(
        "$(secret.endpoint)/authenticate",
        [
            "Authorization" => "Basic $(
                Base64.base64encode("$(secret.username):$(secret.password)")
            )",
        ];
    )
    j = JSON.parse(String(r.body))
    return (;
        config...,
        userword => String(j["token"]),
    )
end

function get(access, resource; kwargs...)
    r = HTTP.get(
        "$(access.endpoint)/$(resource)",
        ["Authorization" => "Key $(access.userword)"];
        kwargs...,
    )
    j = JSON.parse(String(r.body))
    return j
end

function post(access, resource, body; kwargs...)
    j = JSON.json(body)
    r = HTTP.post(
        "$(access.endpoint)/$(resource)",
        ["Authorization" => "Bearer $(access.userword)"],
        j;
        kwargs...,
    )
    j = JSON.parse(String(r.body))
    return j
end

function savemeta(access, date1, date2)
    # datapath/dataname/json/meta/0.json, ...
end

function savemeta(access, dates)
    savemeta(access, dates[1], dates[end])
end

function loadmeta(config)
end

function savedata(access, date1, date2)
    # save json files as
    # datapath/dataname/json/meta/0.json, ...
    # datapath/dataname/json/data/name/name/date.json
end

function savedata(access, dates, meta)
    for (date1, date2) in zip(dates[1 : end - 1], dates[2 : end])
        savedata(access, date1, date2)
    end
end

function loaddata(config)
    # load json files
end

function save(access, dates)
    savemeta(access, dates)
    meta = loadmeta(access)
    savedata(access, dates, meta)
end

function load(config)
    # as arrays of named tuples, etc.
end

end

module Evo

import Dates
import JuliaDB
import Unitful

function savedata(config)
    # update standardised database, i.e., standard names and units of measure
end

function loaddata(config)
    # load and preprocess data using specified units of measure and resolution
end

end
