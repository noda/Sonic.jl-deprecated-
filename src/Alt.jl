module LinckiiAPISecrets

keab = (
    :dataname => "keab",
    :datapath => "data",
    :endpoint => "https://canary.noda.se/~keab/api/v1",
    :passname => "e86d93131e6bc12ae701d036e696cf8b",
)

end

module LinckiiAPI

import HTTP
import JSON

function get_access(secret)
    return (;
        secret...,
    )
end

function get(access, resource; kwargs...)
    r = HTTP.get(
        "$(access.endpoint)/$(resource)",
        ["Authorization" => "Key $(access.passname)"];
        kwargs...
    )
    j = JSON.parse(String(r.body))
    return j
end

function save(access)
    # save json files as
    # datapath/dataname/json/meta/d.json
    # datapath/dataname/json/meta/n.json
    # datapath/dataname/json/meta/s.json
    # datapath/dataname/json/data/name/name/date.json
end

function load(secret, ...)
    # load json files as arrays of named tuples, etc.
end

module Linckii

import Dates
import JuliaDB
import Unitful

function save(access)
    # update standardised database, i.e., standard names and units of measure
end

function load(secret, ...)
    # load data using specified units of measure and resolution
end

end

module EvoAPISecrets

veab = (
    :dataname => "veab",
    :datapath => "data",
    :endpoint => "https://evo.elvaco.se/api/v1",
    :password => "bV6CDHnC&tMm",
    :username => "jens.brage@noda.se",
)

end

module EvoAPI

import HTTP
import JSON

function get_access(secret)
    r = HTTP.get(
        "$(secret.endpoint)/authenticate",
        [
            "Authorization" => "Basic $(
                Base64.base64encode("$(secret.username):$(secret.password)")
            )",
        ];
    )
    j = JSON.parse(String(r.body)
    return (;
        secret...,
        userword => String(j["token"]),
    )
end

function get(access, resource; kwargs...)
    r = HTTP.get(
        "$(access.endpoint)/$(resource)",
        ["Authorization" => "Key $(access.userword)"];
        kwargs...
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
        kwargs...
    )
    j = JSON.parse(String(r.body))
    return j
end

function save(access)
    # save json files as
    # datapath/dataname/json/meta/0.json, ...
    # datapath/dataname/json/data/name/name/date.json
end

function load(secret, ...)
    # load json files as arrays of named tuples, etc.
end

module Evo

import Dates
import JuliaDB
import Unitful

function save(access)
    # update standardised database, i.e., standard names and units of measure
end

function load(secret, ...)
    # load data using specified units of measure and resolution
end

end
