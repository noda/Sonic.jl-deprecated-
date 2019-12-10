module Nodosus

import Dates
import JuliaDB
import Statistics

function remove_repetitions(rows; by = r -> r.value)
    rs = [rows[1]]
    for r in rows[2 : end]
        if by(r) != by(rs[end])
            push!(rs, r)
        end
    end
    return rs
end

function partition(rows; by = identity)
    rss = [[rows[1]]]
    for r in rows[2 : end]
        if by(r) != by(rss[end][end])
            push!(rss, [r])
        else
            push!(rss[end], r)
        end
    end
    return rss
end

"""
    filtertukeysfences(rows; tukeysfences = 3)

Adapted from https://en.wikipedia.org/wiki/Outlier, #Tukey's_fences.
"""
function filtertukeysfences(rows; by = r -> Dates.week(r.datetime), tukeysfences = 3, kwargs...)
    rs = []
    for part in partition(rows; by = by)
        q1, q2, q3 = Statistics.quantile(
            map(r -> r.value, part),
            (0.25, 0.50, 0.75),
        )
        fence1 = q2 + tukeysfences * (q1 - q2)
        fence2 = q2 + tukeysfences * (q3 - q2)
        append!(
            rs,
            filter!(
                r -> (
                    r.value > fence1 &&
                    r.value < fence2
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
    Q = q
    if typeof(Q) <: Dates.Period
        Q = (s, e) -> e.datetime - s.datetime < q
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
    pp0(rows, p, args...; kwargs...)

Preprocess a control signal, assuming, a.e., `derivative(0, signal) == 0`.
"""
function pp0(rows, p, args...; kwargs...)
    return resample(rows, p, args...)
end

"""
    pp1(rows, p, args...; kwargs...)

Preprocess a measure signal, assuming, a.e., `derivative(1, signal) == 0`.
"""
function pp1(rows, p, args...; kwargs...)
    rows = remove_repetitions(rows)
    rows = filtertukeysfences(rows; kwargs...)
    return resample(rows, p, args...)
end

"""
    pp2(rows, p, args...; kwargs...)

Preprocess a measure signal, assuming, a.e., `derivative(2, signal) == 0`.
"""
function pp2(rows, p, args...; kwargs...)
    rows = remove_repetitions(rows; by = r -> r.value)
    return resample(rows, p, args...)
end

end
