# Item Filter Patterns

This document describes how to filter items in the `/items` directory by type.

## Filter Patterns

| Type | JSON Pattern | Filename Pattern |
|------|-------------|------------------|
| **Craft** | Has `"recipe": {...}` (A1-C3 grid) **or** has `"recipes": [{"type":"crafting","slots": {...}}]` | - |
| **Forge** | Has `"recipes": [{"type": "forge", ...}]` | - |
| **Pets** | Has `"petInfo"` in nbttag | `*;0.json` to `*;4.json` |
| **Shards** | - | `*SHARD*.json` |

---

## Detailed Patterns

### Craft Items

Crafted items are identified by either:
1. A top-level `recipe` object with grid positions.
2. A `recipes` array entry with `"type": "crafting"` and a `slots` map.

This matches `src/main/java/com/skyblockflipper/backend/NEU/NEUItemFilterHandler.java`, where craft detection checks both `node.path("recipe")` and entries in `node.path("recipes")`.

```json
"recipe": {
  "A1": "ITEM:1",
  "A2": "ITEM:1",
  "A3": "ITEM:1",
  "B1": "ITEM:1",
  "B2": "ITEM:1",
  "B3": "ITEM:1",
  "C1": "ITEM:1",
  "C2": "ITEM:1",
  "C3": "ITEM:1"
}
```

```json
"recipes": [
  {
    "type": "crafting",
    "slots": {
      "A1": "ITEM:1",
      "B2": "ITEM:2"
    }
  }
]
```

### Forge Items

Items from the Forge have a `recipes` array with `"type": "forge"`:

```json
"recipes": [
  {
    "type": "forge",
    "inputs": ["ITEM:1", "ITEM:2"],
    "count": 1,
    "duration": 86400
  }
]
```

### Pets

Pet files use a semicolon suffix indicating rarity tier:
- `;0` = COMMON
- `;1` = UNCOMMON
- `;2` = RARE
- `;3` = EPIC
- `;4` = LEGENDARY

Example: `BEE;4.json` = Legendary Bee Pet

NBT contains `petInfo`:

```json
"petInfo": "{\"type\":\"BEE\",\"tier\":\"LEGENDARY\",\"exp\":0.0}"
```

### Shards

Shard items have `SHARD` in the filename.

Example: `ATTRIBUTE_SHARD_ALMIGHTY;1.json`

---

## Command Examples

```bash
# Quick approximation for CRAFT items (fast grep, may miss/overmatch):
grep -l '"recipe":' items/*.json

# Accurate CRAFT detection (requires jq):
# Matches .recipe OR .recipes[] entries where .type == "crafting"
for f in items/*.json; do
  jq -e '.recipe? != null or ((.recipes // []) | any(.type == "crafting"))' "$f" >/dev/null && echo "$f"
done

# Find FORGE items
grep -l '"type": "forge"' items/*.json

# Find PETS (by filename)
ls items/*\;[0-4].json

# Find SHARDS
ls items/*SHARD*.json

# Count each type
grep -l '"recipe":' items/*.json | wc -l

# Accurate craft count (requires jq)
for f in items/*.json; do
  jq -e '.recipe? != null or ((.recipes // []) | any(.type == "crafting"))' "$f" >/dev/null && echo "$f"
done | wc -l

grep -l '"type": "forge"' items/*.json | wc -l
ls items/*\;[0-4].json | wc -l
ls items/*SHARD*.json | wc -l
```

---

## Other Recipe Types

The `recipes` array can also contain:
- `"type": "katgrade"` - Pet upgrade via Kat
- `"type": "npc_shop"` - NPC shop purchase
- `"type": "essence_shop"` - Essence shop items


