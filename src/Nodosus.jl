module Nodosus

import Dates
import JuliaDB
import Statistics

function partition(f, xs; by = identity)
    ys = []
    if !isempty(xs)
        s = 1
        b = by(xs[s])
        e = findnext(x -> by(x) != b, xs, s)
        while e != nothing
            push!(ys, f(xs[s : e - 1]))
            s = e
            b = by(xs[s])
            e = findnext(x -> by(x) != b, xs, s)
        end
        push!(ys, f(xs[s : end]))
    end
    return ys
end

"""
    dropreps(rows)

Drop repeted elements.
"""
function dropreps(rows)
    return partition(first, rows; by = r -> r.value)
end

"""
    filtertf(rows; tf = 3)

Adapted from https://en.wikipedia.org/wiki/Outlier, #Tukey's_fences.
"""
function filtertf(rows; by = Dates.week, tf = 3, kwargs...)
    rs = []
    for part in partition(identity, rows; by = r -> by(r.datetime))
        q1, q2, q3 = Statistics.quantile(
            map(r -> r.value, part),
            (0.25, 0.50, 0.75),
        )
        f1 = q2 + tf * (q1 - q2)
        f2 = q2 + tf * (q3 - q2)
        append!(
            rs,
            filter!(
                r -> (
                    r.value > f1 &&
                    r.value < f2
                ),
                part,
            ),
        )
    end
    return rs
end

function interpolate1(s, e, t)
    S = Dates.value(e.datetime - t)
    E = Dates.value(t - s.datetime)
    T = S + E
    S = S / T
    E = E / T
    return (;
        :datetime => t,
        :timezone => Dates.Time(12) + Dates.Nanosecond(
            round(
                S * Dates.value(s.timezone - Dates.Time(12)) +
                E * Dates.value(e.timezone - Dates.Time(12)),
            ),
        ),
        :node_id => s.node_id,
        :variable => s.variable,
        :value => (
            S * s.value +
            E * e.value
        ),
    )
end

function interpolates(s, e, p)
    return (
        interpolate1(s, e, t)
        for t in ceil(s.datetime, p) : p : ceil(e.datetime - p, p)
    )
end

function resample(rows, p, q = (s, e) -> true)
    if typeof(q) <: Dates.Period
        Q = (s, e) -> e.datetime - s.datetime < q
    else
        Q = q
    end
    return collect(
        Iterators.flatten(
            (
                interpolates(s, e, p)
                for (s, e) in zip(rows[1 : end - 1], rows[2 : end])
                if Q(s, e)
            ),
        ),
    )
end

"""
    pp0(data, p, args...; kwargs...)

Preprocess a control signal, assuming, a.e., `derivative(0, signal) == 0`.
"""
function pp0(data, p, args...; kwargs...)
    rows = JuliaDB.rows(data)
    rows = resample(rows, p, args...)
    return JuliaDB.table(
        rows;
        pkey = JuliaDB.pkeynames(data),
    )
end

"""
    pp1(data, p, args...; kwargs...)

Preprocess a measure signal, assuming, a.e., `derivative(1, signal) == 0`.
"""
function pp1(data, p, args...; kwargs...)
    rows = JuliaDB.rows(data)
    rows = dropreps(rows)
    rows = filtertf(rows; kwargs...)
    rows = resample(rows, p, args...)
    return JuliaDB.table(
        rows;
        pkey = JuliaDB.pkeynames(data),
    )
end

"""
    pp2(data, p, args...; kwargs...)

Preprocess a measure signal, assuming, a.e., `derivative(2, signal) == 0`.
"""
function pp2(data, p, args...; kwargs...)
    rows = JuliaDB.rows(data)
    rows = dropreps(rows)
    rows = resample(rows, p, args...)
    return JuliaDB.table(
        rows;
        pkey = JuliaDB.pkeynames(data),
    )
end

end
