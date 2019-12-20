module Nodosus

import Dates
import JuliaDB
import Statistics

"""
    partition(f, xs, by = identity)

Partition series.
"""
function partition(f, xs, by = identity)
    if typeof(by) == Symbol
        by_s = by
        by = r -> r[by_s]
    end
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
    dropre(xs, by)

Drop repetitions.
"""
function dropre(xs, by)
    return partition(first, xs, by)
end

"""
    filtertf(rows; by = Dates.week, tf = 3)

Adapted from https://en.wikipedia.org/wiki/Outlier, #Tukey's_fences.
"""
function filtertf(rows; by = Dates.week, tf = 3)
    rs = []
    for part in partition(identity, rows, r -> by(r.datetime))
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
    return (
        datetime = t,
        value = (
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
        q_period = q
        q = (s, e) -> e.datetime - s.datetime < q_period
    end
    return collect(
        Iterators.flatten(
            [
                interpolates(s, e, p)
                for (s, e) in zip(rows[1 : end - 1], rows[2 : end])
                if q(s, e)
            ],
        ),
    )
end

"""
    preprocess0(rows, p, [q = (s, e) -> true])

Preprocess a control series.
"""
function preprocess0(rows, p, args...)
    rs = sort(rows; by = r -> r.datetime)
    rs = dropre(rs, :datetime)
    rs = resample(rs, p, args...)
    return rs
end

"""
    preprocess1(rows, p, [q = (s, e) -> true]; by = Dates.week, tf = 3)

Preprocess a measure series.
"""
function preprocess1(rows, p, args...; kwargs...)
    rs = sort(rows; by = r -> r.datetime)
    rs = dropre(rs, :datetime)
    rs = dropre(rs, :value)
    rs = filtertf(rs; kwargs...)
    rs = resample(rs, p, args...)
    return rs
end

"""
    preprocess2(rows, p, [q = (s, e) -> true])

Preprocess a measure series of integrated values.
"""
function preprocess2(rows, p, args...)
    rs = sort(rows; by = r -> r.datetime)
    rs = dropre(rs, :datetime)
    rs = dropre(rs, :value)
    rs = resample(rs, p, args...)
    return rs
end

"""
    preprocess(data, pattern)

Preprocess data according to pattern.
"""
function preprocess(data, pattern)
    return JuliaDB.reindex(
        JuliaDB.groupby(data, :variable; flatten = true, usekey = true) do k, rs
            return pattern[k.variable](rs)
        end,
        :datetime,
    )
end

end
