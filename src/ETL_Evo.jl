module ETL_Evo

import Base64
import CSV
import DataFrames
import Dates
import HTTP
import JSON
import Unitful

include("./Nodosus.jl")

function get_access(config)
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
            address = String(json["address"]),
            facility = String(json["facility"]),
            gatewaySerial = String(json["gatewaySerial"]),
            id = String(json["id"]),
            isReported = Bool(json["isReported"]),
            location = json["location"],
            manufacturer = String(json["manufacturer"]),
            medium = String(json["medium"]),
            organisationId = String(json["organisationId"]),
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

"""
loaddata = extract

This function should preferably take care of renaming of variables and the
rescaling to standard units, and answer with (:datetime=,:variable=,:value=).
"""
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

rename = Dict(
    Symbol("Energy")                => :energy,
    Symbol("Flow")                  => :flow,
    Symbol("Forward temperature")   => :supply,
    Symbol("Power")                 => :power,
    Symbol("Return temperature")    => :return,
    Symbol("Volume")                => :volume,
)

"""
The functions download and transform both tend to take a lot of time, and
sometimes even load. It would be good if they could log progress or estimated
time remaining every minute or so. It would also be good if extract also
extracted some metadata to be saved together with the processed data for
presentation purposes.
"""

function download(config, dates...; preprocessor = default_preprocessor)
    access = ETL.get_access(config)
    savemeta(access, dates...)
    meta = loadmeta(config)
    for r in meta
        try
            for (variable, pp) in preprocessor
                savedata(access, r.id, variable, dates...)
            end
        catch
        end
    end
end

function extract(config, resource, variable)
    loaddata(config, resource, variable)
end

function transform(config; preprocessor = default_preprocessor)
    meta = loadmeta(config)
    for r in meta
        p = joinpath(
            config.database,
            "data",
            "$(r.id).csv",
        )
        try
            t = (
                datetime = Dates.DateTime[],
                variable = Symbol[],
                value = Float64[],
            )
            for (variable, pp) in preprocessor
                unit, xs = extract(config, r.id, variable)
                xs = pp(xs, unit)
                for x in xs
                    push!(t.datetime, x.datetime)
                    push!(t.variable, rename[variable])
                    push!(t.value, x.value)
                end
            end
            if isempty(t.datetime)
                error("empty")
            end
            CSV.write(p, DataFrames.unstack(DataFrames.DataFrame(t)))
            println("$(p): transformed")
        catch e
            println("$(p): $(e)")
        end
    end
end

function load(config, resource)
    df = nothing
    p = joinpath(
        config.database,
        "data",
        "$(resource).csv",
    )
    try
        df = CSV.read(p)
        # println("$(p): loaded")
    catch e
        # println("$(p): $(e)")
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
    access = get_access(config)
    download(
        access,
        Dates.DateTime("2020-01-01"),
        Dates.DateTime("2020-01-02"),
    )
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
