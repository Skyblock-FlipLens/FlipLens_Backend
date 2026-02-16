# Item Filter Patterns

This document describes how to filter items in the `/items` directory by type.

## Filter Patterns

| Type | JSON Pattern | Filename Pattern |
|------|-------------|------------------|
| **Craft** | Has `"recipe": {...}` (object with A1-C3 grid) | - |
| **Forge** | Has `"recipes": [{"type": "forge", ...}]` | - |
| **Pets** | Has `"petInfo"` in nbttag | `*;0.json` to `*;4.json` |
| **Shards** | - | `*SHARD*.json` |

---

## Detailed Patterns

### Craft Items
Items crafted at a crafting table have a `recipe` object with grid positions:

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
# Find CRAFT items (crafting table recipe)
grep -l '"recipe":' items/*.json

# Find FORGE items
grep -l '"type": "forge"' items/*.json

# Find PETS (by filename)
ls items/*\;[0-4].json

# Find SHARDS
ls items/*SHARD*.json

# Count each type
grep -l '"recipe":' items/*.json | wc -l
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
