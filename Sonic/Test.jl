
module Linckii

struct Connection
end

struct Side2
    temp1::Symbol # temperature, supply
    temp2::Symbol # temperature, return
    temp3::Set{Symbol}
end

struct Side1
    power::Symbol # kW forwcast
    power::Symbol # kW
    temp1::Symbol # temperature, supply
    temp2::Symbol # temperature, return
    temp3::Symbol # temperature, outside forecast
    temp4::Symbol # temperature, outside
    temp5::Symbol # temperature, outside requested offset
    temp6::Symbol # temperature, outside responded offset
    side2::Dict{Symbol, Side2}
end

struct Data1
    power::Symbol # finns inte dedikerat forecast
    temp1::Symbol # temperature, supply
    temp2::Symbol # temperature, return
    temp3::Symbol # temperature, outside forecast
    side1::Dict{Symbol, Side1}
end

struct Data2
end

function get_data1(connection::Connection)::Data1
    "do stuff"
end

function get_data2(connection::Connection, data1::Data1)::Data2
    "do stuff"
end

function set_data2(connection::Connection, data2::Data2)
    "do stuff"
end
end
