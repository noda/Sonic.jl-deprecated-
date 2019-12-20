module Evo

import Base64
import Dates
import HTTP
import JSON
import JuliaDB
import Unitful

function get_access(secret)
    response = HTTP.get(
        "$(secret.source.endpoint)/authenticate",
        [
            "Authorization" => "Basic $(
                Base64.base64encode(
                    "$(secret.source.username):$(secret.source.password)",
                )
            )",
        ];
    )
    return (
        source = secret.source,
        target = secret.target,
        bearer = String(JSON.parse(String(response.body))["token"]),
    )
end

function get(access, resource_name; kwargs...)
    response = HTTP.get(
        "$(access.source.endpoint)/$(resource_name)",
        ["Authorization" => "Bearer $(access.bearer)"];
        kwargs...
    )
    return JSON.parse(String(response.body))
end

function get_meters(access, date1, date2, page; kwargs...)
    query = [
        "medium"    => "District heating",
        "after"     => string(date1),
        "before"    => string(date2),
        "size"      => 50,
        "page"      => page,
    ]
    return get(access, "meters"; query = query, kwargs...)
end

function get_content(access, date1, date2, page; kwargs...)
    return map(
        json -> (
            collectionPercentage = Float64(json["collectionPercentage"]),
            facility = Symbol(json["facility"]),
            gatewaySerial = Symbol(json["gatewaySerial"]),
            id = Symbol(json["id"]),
            isReported = Bool(json["isReported"]),
            location = json["location"],
            # location = let json = json["location"] (
            #     address = Symbol(json["address"]),
            #     city = Symbol(json["city"]),
            #     country = Symbol(json["country"]),
            #     position = let json = json["position"] (
            #         latitude = Float64(json["latitude"]),
            #         longitude = Float64(json["longitude"]),
            #         confidence = Float64(json["confidence"]),
            #     ),
            #     zip = Symbol(json["zip"]),
            # ),
            manufacturer = Symbol(json["manufacturer"]),
            medium = Symbol(json["medium"]),
            organisationId = Symbol(json["organisationId"]),
            readIntervalMinutes = Int64(json["readIntervalMinutes"]),
        ),
        get_meters(access, date1, date2, page; kwargs...)["content"],
    )
end

function get_content(access, date1, date2; kwargs...)
    content = []
    flag = true
    page = 0
    while flag
        pagecontent = get_content(access, date1, date2, page; kwargs...)
        push!(content, pagecontent)
        print(" $(page),")
        flag = !isempty(pagecontent)
        page = page + 1
    end
    return collect(Iterators.flatten(content)) # JuliaDB.table(content; pkey = :id)
end

function post(access, resource_name, body; kwargs...)
    response = HTTP.post(
        "$(access.source.endpoint)/$(resource_name)",
        [
            "Authorization" => "Bearer $(access.bearer)",
            "Content-Type"  => "application/json",
        ],
        body;
        kwargs...
    )
    return JSON.parse(String(response.body))
end

function get_datetime(ts)
    return Dates.unix2datetime(ts)
end

sensor_units = Dict(
    Symbol("Difference temperature")    => Unitful.K,
    Symbol("Energy")                    => Unitful.hr * Unitful.MW,
    Symbol("Flow")                      => Unitful.l / Unitful.hr,
    Symbol("Forward temperature")       => Unitful.°C,
    Symbol("Power")                     => Unitful.kW,
    Symbol("Return temperature")        => Unitful.°C,
    Symbol("Volume")                    => Unitful.m^3,
)

return_units = Dict(
    Symbol("K")                         => Unitful.K,
    Symbol("kWh")                       => Unitful.hr * Unitful.kW,
    Symbol("m³/h")                      => Unitful.m^3 / Unitful.hr,
    Symbol("°C")                        => Unitful.°C,
    Symbol("W")                         => Unitful.W,
    Symbol("m³")                        => Unitful.m^3,
)

function get_row(d, v)
    datetime = get_datetime(v["when"])
    # node_id = Symbol(d["id"])
    variable = Symbol(d["quantity"])
    unit = Symbol(d["unit"])
    value = Float64(v["value"])
    return (
        datetime = datetime,
        # node_id = node_id,
        variable = variable,
        value = Unitful.ustrip(
            sensor_units[variable](return_units[unit](value)),
        ),
    )
end

function get_rows(json)
    return [
        get_row(d, v)
        for d in json
        for v in d["values"]
        if "value" in keys(v)
    ]
end

function get_data(access, node_id, sensor_name, date1, date2; kwargs...)
    return get_rows(
        post(
            access,
            "measurements",
            JSON.json(
                Dict(
                    "logicalMeterId"    => [node_id],
                    "quantity"          => ["$(sensor_name)::readout"],
                    "reportAfter"       => "$(Dates.DateTime(date1))+00:00",
                    "reportBefore"      => "$(Dates.DateTime(date2))+00:00",
                    "resolution"        => "hour",
                ),
            );
            kwargs ...
        ),
    )
end

function get_data(access, node_id, sensor_name, dates; kwargs...)
    return collect(
        Iterators.flatten(
            [
                get_data(access, node_id, sensor_name, s, e; kwargs...)
                for (s, e) in zip(dates[1 : end - 1], dates[2 : end])
            ],
        ),
    )
end

function dbpath(args...; db = nothing)
    args = map(arg -> "$(arg)", args)
    args = isnothing(db) ? args : (args..., "$(db).db")
    return joinpath(args...)
end

function savesite(secret; kwargs...)
    p = secret.target
    mkpath(p)
    for (k, v) in kwargs
        JuliaDB.save(v, dbpath(p; db = k))
    end
end

function loadsite(secret, args...)
    p = secret.target
    return (;
        (
            k => JuliaDB.load(dbpath(p; db = k))
            for k in args
        )...
    )
end

function savedata(access, node_id, sensor_name, dates...; kwargs...)
    rows = get_data(access, node_id, sensor_name, dates...; kwargs...)
    data = JuliaDB.table(rows, pkey = :datetime)
    p = secret.target
    p = dbpath(p, "data", node_id)
    mkpath(p)
    JuliaDB.save(
        data,
        dbpath(p; db = sensor_name),
    )
end

function loaddata(secret, node_id, sensor_name :: Symbol)
    p = secret.target
    p = dbpath(p, "data", node_id)
    return JuliaDB.load(
        dbpath(p; db = sensor_name),
    )
end

function loaddata(secret, node_id, sensor_names)
    return foldl(
        JuliaDB.merge,
        [
            loaddata(secret, node_id, sensor_name)
            for sensor_name in sensor_names
        ],
    )
end

end
