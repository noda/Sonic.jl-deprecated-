module Nodosus

import Dates
import Statistics

function remove_repetitions(rows)
    rows = sort(rows, r -> r.datetime)
    return Iterators.flatten(
        (
            (rows[1],),
            Iterators.flatten(
                map(
                    (s, e) -> s.value == e.value ? () : (e,),
                    zip(rows[1 : end - 1], rows[2 : end]),
                ),
            ),
        ),
    )
end

"""
    filtertukeysfences(rows; tukeysfences = 3)

Adapted from https://en.wikipedia.org/wiki/Outlier, #Tukey's_fences.
"""
function filtertukeysfences(rows; tukeysfences = 3)
    q1, q2, q3 = Statistics.quantile(
        (r.values for r in rows),
        (0.25, 0.50, 0.75),
    )
    fence1 = q2 + tukeysfences * (q1 - q2)
    fence2 = q2 + tukeysfences * (q3 - q2)
    return (
        r
        for r in rows
        if (
            r.value > fence1 &&
            r.value < fence2
        )
    )
end

function groupby(rows, by)
    groups = Dict()
    for r in rows
        push!(get!(groups, by(r), Set()), r)
    end
    return groups
end

function filtertukeysfences(rows; tukeysperiod = Dates.Week, kwargs...)
    return Iterators.flatten(
        (
            filtertukeysfences(rows; kwargs...)
            for rows in values(groupby(rows, by))
        ),
    )
end

function resample(s, e, t)
    S = Dates.value(e.datetime - t)
    E = Dates.value(t - s.datetime)
    T = S + E
    return (;
        :datetime => t,
        :timezone => (
            S * (s.timezone - Dates.Hour(12)) +
            E * (e.timezone - Dates.Hour(12))
        ) / T + Dates.Hour(12),
        :value => (
            S * s.value +
            E * e.value
        ) / T,
    )
end

function resample(s, e, p)
    return (
        resample(s, e, t)
        for t in ceil(s.datetime, p) : p : ceil(e.datetime - p, p)
    )
end

function resample(rows, p, q = (s, e) -> true)
    rows = sort(rows, r -> r.datetime)
    if typeof(q) <: Dates.Period
        q = (s, e) -> e.datetime - s.datetime < q
    end
    Iterators.flatten(
        (
            resample(s, e, period)
            for (s, e) in zip(rows[1 : end - 1], rows[2 : end])
            if q(s, e)
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
    rows = remove_repetitions(rows)
    return resample(rows, p, args...)
end

end
