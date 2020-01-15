module LinckiiConfigs

keab = (
    database = joinpath("database", "keab"),
    endpoint = "https://canary.noda.se/~keab",
    passname = "SECRET",
)

end

module Linckii

import CSV
import DataFrames
import Dates
import HTTP
import JSON
import Unitful

include("./Nodosus.jl")

function GET_access(config)
    (;
        config...,
    )
end

function GET(access, args...; kwargs...)
    HTTP.get(
        "$(access.endpoint)/api/v1/$(join(args, "/"))",
        ["Authorization" => "Key $(access.passname)"];
        kwargs...,
    )
end

function savemeta(access, resource)
    r = joinpath(
        access.database,
        "meta",
    )
    mkpath(r)
    p = joinpath(
        r,
        "$(resource).json",
    )
    if !isfile(p)
        try
            write(p, String(GET(access, resource).body))
            # println("$(p): downloaded")
        catch e
            # println("$(p): $(e)")
            rethrow(e)
        end
    end
end

meta_transform_unit = Dict(
    "&deg;h"    => Unitful.hr * Unitful.K,
    "&deg;"     => Unitful.NoUnits,
    "MWh"       => Unitful.hr * Unitful.MW,
    "l/h"       => Unitful.l * Unitful.hr ^ -1,
    "Sec"       => Unitful.s,
    "bar"       => Unitful.bar,
    "bool"      => Unitful.NoUnits,
    "bit"       => Unitful.NoUnits,
    "int"       => Unitful.NoUnits,
    "m/s"       => Unitful.m * Unitful.s ^ -1,
    "m³"        => Unitful.m ^ 3,
    "%"         => Unitful.percent,
    "&deg;C"    => Unitful.°C,
    "°C"        => Unitful.°C,
    "m3"        => Unitful.m ^ 3,
    "°"         => Unitful.°,
    "kW"        => Unitful.kW,
    "kWh"       => Unitful.hr * Unitful.kW,
    ""          => Unitful.NoUnits,
)

meta_transform_dict = Dict(
    "device" => (
        Dict(
            :device_protocol_id => :protocol_id,
        ),
        Dict(
            :device_name => s -> Symbol(s),
        ),
    ),
    "sensor" => (
        Dict(
            :sensor_postfix => :sensor_unit,
            :sensor_protocol_id => :protocol_id,
        ),
        Dict(
            :sensor_name => s -> Symbol(s),
            :sensor_unit => s -> meta_transform_unit[s],
        ),
    ),
    "node" => (
        Dict(
            :node_device => :device_id,
            :node_sensor_ids => :sensor_ids,
        ),
        Dict(
            :node_name => s -> Symbol(s),
            :sensor_ids => sensor_ids -> map(Int64, sensor_ids),
        ),
    ),
)

function meta_transform(resource)
    tk, tv = meta_transform_dict[resource]
    json -> map(
        json -> (;
            (
                k => get(tv, k, identity)(v)
                for (k, v) in (
                    get(tk, k, k) => v
                    for (k, v) in (
                        Symbol("$(resource)_$(k)") => v
                        for (k, v) in json
                    )
                )
            )...,
        ),
        json["$(resource)s"],
    )
end

function loadmeta(config, resource)
    re = meta_transform(resource)
    rs = []
    p = joinpath(
        config.database,
        "meta",
        "$(resource).json",
    )
    try
        j = JSON.parse(String(read(p)))
        append!(rs, re(j))
        # println("$(p): loaded")
    catch e
        # println("$(p): $(e)")
        rethrow(e)
    end
    rs
end

function savedata(access, resource, variable, dates...)
    r = joinpath(
        access.database,
        "data",
        "$(resource)",
        "$(variable)",
    )
    mkpath(r)
    for (d1, d2) in zip(dates[1 : end - 1], dates[2 : end])
        p = joinpath(
            r,
            "$(d1).json",
        )
        if !isfile(p)
            q = [
                "node_id" => "$(resource)",
                "tag" => "$(variable)",
                "start" => "$(d1)",
                "end" => "$(d2)",
            ]
            try
                write(p, String(GET(access, "timeseries";  query=q).body))
                # println("$(p): downloaded")
            catch e
                # println("$(p): $(e)")
                rethrow(e)
            end
        end
    end
end

function data_datetime(timestamp)
    Dates.DateTime(timestamp[1:19])
end

"""
    data_timezone(timestamp)

The timezone and the implisit DST are important for modelling social behaviours,
but it would be better to have them as a separate signal rather than  as part of
the timestamp. Or reconstruct from elsewhere.

As a workaround for datetime-timezone arithmetics, encode the timezone offset by
12:00:00±HH:MM.
"""
function data_timezone(timestamp)
    t = Dates.Time(12)
    z = Dates.Time(timestamp[end - 4 : end]) - Dates.Time(0)
    timestamp[end - 5] == '-' ? t - z : t + z
end

function data_transform(json)
    map(
        json -> (
            datetime = data_datetime(json["ts"]),
            value = Float64(json["v"]),
        ),
        (j2 for j1 in json["timeseries"] for j2 in j1["data"]),
    )
end

function loaddata(config, resource, variable)
    rs = []
    r = joinpath(
        config.database,
        "data",
        "$(resource)",
        "$(variable)",
    )
    for s in filter(s -> endswith(s, ".json"), readdir(r))
        p = joinpath(r, s)
        try
            j = JSON.parse(String(read(p)))
            append!(rs, data_transform(j))
            # println("$(p): loaded")
        catch e
            # println("$(p): $(e)")
            rethrow(e)
        end
    end
    rs
end

default_preprocessor = let
    p, q = Dates.Minute(60), Dates.Minute(4 * 60)
    pp0 = (rows, unit) -> Nodosus.preprocess0(rows, unit, p, q)
    pp1 = (rows, unit) -> Nodosus.preprocess1(rows, unit, p, q)
    pp2 = (rows, unit) -> Nodosus.preprocess2(rows, unit, p, q)
    Dict(
        4 => Dict(
            :meter_effect           => pp1,
            :meter_heatenergy       => pp2,
            :meter_primreturntemp   => pp1,
            :meter_primsupplytemp   => pp1,
            :meter_volume           => pp1,
            :meter_volumeflow       => pp2,
            :outdoortemp            => pp1,
            :outdoortemp_offset     => pp0,
        ),
        6 => Dict(
            :cloudiness             => pp0,
            :outdoortemp            => pp0,
            :wind_direction         => pp0,
            :wind_speed             => pp0,
        ),
    )
end

function save(access, dates...; preprocessor = default_preprocessor)
    meta = (;
        (
            Symbol(r) => begin
                savemeta(access, r)
                loadmeta(access, r)
            end
            for r in ["device", "sensor", "node"]
        )...,
    )
    prot = Dict(
        r.device_id => r.protocol_id
        for r in meta.device
    )
    unit = Dict(
        (r.protocol_id, r.sensor_name) => r.sensor_unit
        for r in meta.sensor
    )
    name = Dict(
        r.sensor_id => r.sensor_name
        for r in meta.sensor
    )
    node = filter(
        r -> (
            r.device_id in keys(preprocessor) &&
            Set(name[sensor_id] for sensor_id in r.sensor_ids) >=
            keys(preprocessor[r.device_id])
        ),
        meta.node,
    )
    for r in node
        p = joinpath(
            access.database,
            "data",
            "$(r.node_id).csv",
        )
        if !isfile(p)
            try
                t = (
                    datetime = Dates.DateTime[],
                    variable = Symbol[],
                    value = Float64[],
                )
                for (sensor_name, pp) in preprocessor[r.device_id]
                    savedata(access, r.node_id, sensor_name, dates...)
                    xs = loaddata(access, r.node_id, sensor_name)
                    xs = pp(xs, unit[prot[r.device_id], sensor_name])
                    for x in xs
                        push!(t.datetime, x.datetime)
                        push!(t.variable, sensor_name)
                        push!(t.value, x.value)
                    end
                end
                if isempty(t.datetime)
                    error("empty")
                end
                CSV.write(p, DataFrames.unstack(DataFrames.DataFrame(t)))
                println("$(p): saved")
            catch e
                println("$(p): $(e)")
            end
        end
    end
end

function load(config, node_id)
    df = nothing
    p = joinpath(
        config.database,
        "data",
        "$(node_id).csv",
    )
    try
        df = CSV.read(p)
        println("$(p): loaded")
    catch e
        println("$(p): $(e)")
        rethrow(e)
    end
    df
end

function load(config)
    d = Dict()
    r = joinpath(
        config.database,
        "data",
    )
    for s in filter(s -> endswith(s, ".csv"), readdir(r))
        try
            node_id = parse(Int64, s[1 : end - 4])
            d[node_id] = load(config, node_id)
        catch
        end
    end
    d
end

function test(config)
    if isdir(joinpath(config.database, "data"))
        mv(
            joinpath(config.database, "data"),
            joinpath(config.database, "data.backup"),
        )
    end
    if isdir(joinpath(config.database, "test"))
        rm(
            joinpath(config.database, "test");
            recursive = true,
        )
    end
    access = GET_access(config)
    save(access, Dates.DateTime("2020-01-01"), Dates.DateTime("2020-01-02"))
    load(config)
    mv(
        joinpath(config.database, "data"),
        joinpath(config.database, "test"),
    )
    if isdir(joinpath(config.database, "data.backup"))
        mv(
            joinpath(config.database, "data.backup"),
            joinpath(config.database, "data"),
        )
    end
end

end

module EvoConfigs

veab = (
    database = joinpath("database", "veab"),
    endpoint = "https://evo.elvaco.se",
    password = "SECRET",
    username = "jens.brage@noda.se",
)

end

module Evo

import Base64
import CSV
import DataFrames
import Dates
import HTTP
import JSON
import Unitful

include("./Nodosus.jl")

function GET_access(config)
    r = HTTP.get(
        "$(config.endpoint)/api/v1/user-service/authenticate",
        [
            "Authorization" => "Basic $(
                Base64.base64encode("$(config.username):$(config.password)")
            )",
        ];
    )
    j = JSON.parse(String(r.body))
    return (;
        config...,
        userword = String(j["token"]),
    )
end

function GET(access, resource; kwargs...)
    return HTTP.get(
        "$(access.endpoint)/api/internal/v1/$(resource)",
        ["Authorization" => "Bearer $(access.userword)"];
        kwargs...,
    )
end

function POST(access, resource, body; kwargs...)
    j = JSON.json(body)
    return HTTP.post(
        "$(access.endpoint)/api/internal/v1/$(resource)",
        [
            "Authorization" => "Bearer $(access.userword)",
            "Content-Type"  => "application/json",
        ],
        j;
        kwargs...,
    )
end

function savemeta(access, dates...)
    r = joinpath(
        access.database,
        "meta",
    )
    mkpath(r)
    x = true
    i = 0
    while x
        p = joinpath(
            r,
            "$(i).json",
        )
        if !isfile(p)
            q = [
                "medium" => "District heating",
                "after" => "$(dates[1])",
                "before" => "$(dates[end])",
                "size" => "50",
                "page" => "$(i)",
            ]
            try
                s = String(GET(access, "meters"; query = q).body)
                x = !isempty(JSON.parse(s)["content"])
                write(p, s)
                # println("$(p): downloaded")
            catch e
                # println("$(p): $(e)")
                rethrow(e)
            end
        end
        i = i + 1
    end
end

function meta_transform(json)
    map(
        json -> (
            # address = parse(Int64, String(json["address"])),
            # facility = parse(Int64, String(json["facility"])),
            # gatewaySerial = parse(Int64, String(json["gatewaySerial"])),
            id = String(json["id"]),
            isReported = Bool(json["isReported"]),
            # location = json["location"],
            # manufacturer = String(json["manufacturer"]),
            # medium = String(json["medium"]),
            # organisationId = String(json["organisationId"]),
            readIntervalMinutes = Dates.Minute(Int64(json["readIntervalMinutes"])),
        ),
        json["content"],
    )
end

function loadmeta(config)
    rs = []
    r = joinpath(
        config.database,
        "meta",
    )
    for s in readdir(r)
        p = joinpath(r, s)
        try
            j = JSON.parse(String(read(p)))
            append!(rs, meta_transform(j))
            # println("$(p): loaded")
        catch e
            # println("$(p): $(e)")
            rethrow(e)
        end
    end
    rs
end

function savedata(access, resource, variable, dates...)
    r = joinpath(
        access.database,
        "data",
        "$(resource)",
        "$(variable)",
    )
    mkpath(r)
    for (d1, d2) in zip(dates[1 : end - 1], dates[2 : end])
        p = joinpath(
            r,
            "$(d1).json",
        )
        if !isfile(p)
            b = Dict(
                "logicalMeterId" => ["$(resource)"],
                "quantity" => ["$(variable)::readout"],
                "reportAfter" => "$(Dates.DateTime(d1))+00:00",
                "reportBefore" => "$(Dates.DateTime(d2))+00:00",
                "resolution" => "hour",
            )
            try
                write(p, String(POST(access, "measurements", b).body))
                # println("$(p): downloaded")
            catch e
                # println("$(p): $(e)")
                rethrow(e)
            end
        end
    end
end

data_transform_unit = Dict( # inspect Evo in Chrome
    "°C"    => Unitful.°C,
    "K"     => Unitful.K,
    "kWh"   => Unitful.hr * Unitful.kW,
    "W"     => Unitful.W,
    "m³"    => Unitful.m ^ 3,
    "m³/h"  => Unitful.m ^ 3 / Unitful.hr,
)

function data_datetime(ts)
    return Dates.unix2datetime(ts)
end

function data_transform(json)
    if length(json) != 1
        error("length(json) == $(length(json))")
    end
    json = json[1]
    data_transform_unit[json["unit"]],
    map(
        json -> (
            datetime = data_datetime(json["when"]),
            value = Float64(json["value"]),
        ),
        (j1 for j1 in json["values"] if "value" in keys(j1)),
    )
end

function loaddata(config, resource, variable)
    rs = nothing
    r = joinpath(
        config.database,
        "data",
        "$(resource)",
        "$(variable)",
    )
    for s in filter(s -> endswith(s, ".json"), readdir(r))
        p = joinpath(r, s)
        try
            j = JSON.parse(String(read(p)))
            rs = data_transform(j)
            # println("$(p): loaded")
        catch e
            # println("$(p): $(e)")
            rethrow(e)
        end
    end
    rs
end

default_preprocessor = let
    p, q = Dates.Minute(60), Dates.Minute(4 * 60)
    pp1 = (rows, unit) -> Nodosus.preprocess1(rows, unit, p, q)
    pp2 = (rows, unit) -> Nodosus.preprocess2(rows, unit, p, q)
    Dict(
        Symbol("Energy")                => pp2,
        Symbol("Flow")                  => pp1,
        Symbol("Forward temperature")   => pp1,
        Symbol("Power")                 => pp1,
        Symbol("Return temperature")    => pp1,
        Symbol("Volume")                => pp2,
    )
end

function save(access, dates...; preprocessor = default_preprocessor)
    savemeta(access, dates...)
    meta = loadmeta(access)
    for r in meta
        p = joinpath(
            access.database,
            "data",
            "$(r.id).csv",
        )
        if !isfile(p)
            try
                t = (
                    datetime = Dates.DateTime[],
                    variable = Symbol[],
                    value = Float64[],
                )
                for (variable, pp) in preprocessor
                    savedata(access, r.id, variable, dates...)
                    unit, xs = loaddata(access, r.id, variable)
                    xs = pp(xs, unit)
                    for x in xs
                        push!(t.datetime, x.datetime)
                        push!(t.variable, variable)
                        push!(t.value, x.value)
                    end
                end
                if isempty(t.datetime)
                    error("empty")
                end
                CSV.write(p, DataFrames.unstack(DataFrames.DataFrame(t)))
                println("$(p): saved")
            catch e
                println("$(p): $(e)")
            end
        end
    end
end

function load(config, resource)
    df = nothing
    p = joinpath(
        access.database,
        "data",
        "$(resource).csv",
    )
    try
        df = CSV.read(p)
        println("$(p): loaded")
    catch e
        println("$(p): $(e)")
        rethrow(e)
    end
    df
end

function load(config)
    d = Dict()
    r = joinpath(
        access.database,
        "data",
    )
    for s in filter(s -> endswith(s, ".csv"), readdir(p))
        try
            resource = s[1 : end - 4]
            d[resource] = load(config, resource)
        catch
        end
    end
    d
end

function test(config)
    if isdir(joinpath(config.database, "data"))
        mv(
            joinpath(config.database, "data"),
            joinpath(config.database, "data.backup"),
        )
    end
    if isdir(joinpath(config.database, "test"))
        rm(
            joinpath(config.database, "test");
            recursive = true,
        )
    end
    access = GET_access(config)
    save(access, Dates.DateTime("2020-01-01"), Dates.DateTime("2020-01-02"))
    load(config)
    mv(
        joinpath(config.database, "data"),
        joinpath(config.database, "test"),
    )
    if isdir(joinpath(config.database, "data.backup"))
        mv(
            joinpath(config.database, "data.backup"),
            joinpath(config.database, "data"),
        )
    end
end

end

module EnerpipeConfigs

winsbach = (
    database = joinpath("database", "winsbach"),
)

end

module Enerpipe

function default_preprocessor()
end

function load(config; preprocessor = default_preprocessor)
end

end
