module Evo

import Base64
import Dates
import HTTP
import JSON
import JuliaDB
import Unitful

function get_access(secret)
    response = HTTP.get(
        join(
            [
                "$(secret.url)/api/v1/authenticate",
                ["&$(k)=$(v)" for (k, v) in kwargs]...
            ],
        );
        headers = Dict(
            "Authorization" => "Basic $(
                Base64.base64encode("$(secret.username):$(secret.password)")
            )",
        ),
    )
    return (
        url = secret.url,
        bearer = JSON.parse(String(response.body))["token"],
    )
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
            "Authorization" => "Bearer $(access.bearer)",
        ),
    )
    return JSON.parse(String(response.body))
end

function get_meters(access; medium = "District heating", size = 50, pages = 0, kwargs...) # get_sensors
    json = get("meters", access; medium = medium, size = size, pages = pages, kwargs...)
    return json
end

function get_data(access, uuid; kwargs...)
    json = get("meter/$(uuid)", access; kwargs ...)
    return json
end

end
