# Sonic.jl
Julia package for analytics and control.

Example use in Julia REPL:

```
julia> activate(".")

julia> instantiate()

julia> include("./src/Linckii.jl")

julia> include("../src/LinckiiSecrets.jl")

julia> access = Linckii.get_access(LinckiiSecrets.keab)

julia> Sonic.Linckii.get_nodes(access)
Table with 279 rows, 8 columns:
Columns:
#  colname           type
─────────────────────────────────
1  node_public       Bool
2  node_name         String
3  device_id         Int64
4  node_id           Int64
5  node_owner        Bool
6  node_enabled      Bool
7  sensor_ids        Array{Any,1}
8  node_description  String
```
