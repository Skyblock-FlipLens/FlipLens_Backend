# NotEnoughUpdates Item Database Documentation

Version/Commit: `faa6093`  
Last updated: `2026-02-16`  
Complete documentation for the `/items` directory containing approximately **7,900+ JSON files** (count is approximate and may change).

---

## Table of Contents

1. [Overview](#overview)
2. [JSON Structure](#json-structure)
3. [Recipe Types](#recipe-types)
4. [Item Categories](#item-categories)
5. [Rarity Tiers](#rarity-tiers)
6. [Filter Patterns](#filter-patterns)
7. [Special Fields](#special-fields)
8. [Minecraft Item IDs](#minecraft-item-ids)
9. [Island Locations](#island-locations)
10. [Slayer Requirements](#slayer-requirements)
11. [Command Examples](#command-examples)

---

## Overview

The NotEnoughUpdates (NEU) item database is the primary data source for craft flip ingestion and craft flip calculations in the Skyblock Flipper backend pipeline.

Key data pulled from NEU includes:
- item recipes (both direct `recipe` grids and structured `recipes` entries)
- NPC shop prices and purchase recipes
- forge process durations and inputs
- item metadata (IDs, lore, rarity, category, NBT payloads)
- recipe dependencies used to build multi-step ingredient graphs

Use this document as the reference when implementing, extending, or debugging NEU-based craft flip ingestion and normalization.

---

## JSON Structure

Every item file contains these core fields:

```json
{
  "itemid": "minecraft:skull",
  "displayname": "§6Item Name",
  "nbttag": "{...NBT data...}",
  "damage": 3,
  "lore": ["§7Line 1", "§7Line 2"],
  "internalname": "ITEM_ID",
  "clickcommand": "viewrecipe",
  "crafttext": "",
  "modver": "2.1.0-REL",
  "infoType": "WIKI_URL",
  "info": ["https://wiki.hypixel.net/..."]
}
```

| Field | Description |
|-------|-------------|
| `itemid` | Minecraft item ID |
| `displayname` | Display name with color codes |
| `nbttag` | NBT data (textures, attributes) |
| `damage` | Item damage/metadata value |
| `lore` | Array of lore lines |
| `internalname` | Unique ID (matches filename) |
| `clickcommand` | Action when clicked |
| `crafttext` | Crafting requirements text |
| `modver` | Mod version that added/updated |
| `infoType` | Type of info (usually WIKI_URL) |
| `info` | Wiki URLs |

---

## Recipe Types

### 1. Crafting Table (`recipe`)

Items crafted at a 3x3 crafting grid.

**Count:** ~2,650 items with `viewrecipe`

```json
"recipe": {
  "A1": "ITEM:1", "A2": "ITEM:1", "A3": "ITEM:1",
  "B1": "ITEM:1", "B2": "ITEM:1", "B3": "ITEM:1",
  "C1": "ITEM:1", "C2": "ITEM:1", "C3": "ITEM:1"
}
```

### 2. Forge (`type: "forge"`)

Items crafted at the Forge.

**Count:** 116 items

```json
"recipes": [{
  "type": "forge",
  "inputs": ["ITEM:1", "ITEM:2"],
  "count": 1,
  "duration": 86400,
  "overrideOutputId": "ITEM_ID"
}]
```

### 3. NPC Shop (`type: "npc_shop"`)

Items bought from NPCs.

**Count:** 947 items

```json
"recipes": [{
  "type": "npc_shop",
  "cost": ["SKYBLOCK_COIN:500"],
  "result": "ITEM_ID"
}]
```

### 4. Drops (`type: "drops"`)

Items dropped by mobs/sea creatures.

**Count:** 386 items

```json
"recipes": [{
  "type": "drops",
  "name": "§cMob Name",
  "level": 35,
  "coins": 60,
  "xp": 32,
  "render": "MobType",
  "panorama": "hub",
  "extra": ["§7Requirements..."],
  "drops": [
    {"id": "ITEM_ID", "chance": "100%"},
    {"id": "RARE_DROP", "chance": "0.5%"}
  ]
}]
```

### 5. Kat Upgrade (`type: "katgrade"`)

Pet upgrades via Kat.

**Count:** 185 items

```json
"recipes": [{
  "type": "katgrade",
  "coins": 450000,
  "time": 259200,
  "input": "PET;3",
  "output": "PET;4",
  "items": ["ENCHANTED_GOLD_BLOCK:9"]
}]
```

### 6. Trade (`type: "trade"`)

Items from villager trades.

**Count:** 78 items

```json
"recipes": [{
  "type": "trade",
  "cost": "EMERALD:25",
  "result": "ITEM_ID"
}]
```

### 7. Crafting (recipes array) (`type: "crafting"`)

Alternative crafting format.

**Count:** 296 items

```json
"recipes": [{
  "type": "crafting",
  "slots": {...}
}]
```

---

## Item Categories

### By Filename Pattern

| Category | Pattern | Count |
|----------|---------|-------|
| NPCs | `*_NPC.json` | 441 |
| Generators | `*GENERATOR*.json` | 705 |
| Armor | `*_HELMET.json`, `*_CHESTPLATE.json`, `*_LEGGINGS.json`, `*_BOOTS.json` | 615 |
| Pets | `*;0.json` to `*;4.json` | ~500+ |
| Shards | `*SHARD*.json` | ~100+ |
| Enchantments | `NAME;1.json` to `NAME;10.json` | ~700 |
| Swords | `*SWORD*.json` | 45 |
| Dyes | `*DYE*.json` | 62 |
| Scrolls | `*SCROLL*.json` | 39 |
| Talismans/Rings/Artifacts | `*TALISMAN*.json`, `*RING*.json`, `*ARTIFACT*.json` | 296 |

### By Lore Content

| Category | Grep Pattern | Count |
|----------|--------------|-------|
| Accessories | `"ACCESSORY"` in lore | 374 |
| Cosmetics | `"COSMETIC"` in lore | 686 |
| Dungeon Items | `"DUNGEON"` in lore | 272 |
| Reforge Stones | `"REFORGE STONE"` in lore | 79 |

---

## Rarity Tiers

Items use color codes for rarity (found in last lore line):

| Rarity | Color Code | Example | Count |
|--------|-----------|---------|-------|
| COMMON | `§f§l` | `§f§lCOMMON` | 2,528 |
| UNCOMMON | `§a§l` | `§a§lUNCOMMON` | 1,080 |
| RARE | `§9§l` | `§9§lRARE` | 2,462 |
| EPIC | `§5§l` | `§5§lEPIC` | 862 |
| LEGENDARY | `§6§l` | `§6§lLEGENDARY` | 498 |
| MYTHIC | `§d§l` | `§d§lMYTHIC` | 80 |
| SPECIAL | `§c§l` | `§c§lSPECIAL` | 250 |

### Item Type Suffixes

Rarity can be combined with item type:
- `§9§lRARE ACCESSORY`
- `§6§lLEGENDARY HELMET`
- `§5§lEPIC DUNGEON ITEM`
- `§d§lMYTHIC COSMETIC`
- `§c§lSPECIAL HATCESSORY`

---

## Filter Patterns

### Quick Reference

| Type | JSON Field | Filename |
|------|------------|----------|
| Craft | `"recipe": {...}` | - |
| Forge | `"type": "forge"` | - |
| Pets | `petInfo` in nbttag | `*;[0-4].json` |
| Shards | - | `*SHARD*.json` |
| NPCs | `island`, `x`, `y`, `z` | `*_NPC.json` |
| Enchants | `"parent":`, `enchantments` in nbttag | `NAME;[1-10].json` |
| Vanilla | `"vanilla": true` | - |

---

## Special Fields

### Pet Fields

```json
"nbttag": "{petInfo:\"{\\\"type\\\":\\\"BEE\\\",\\\"tier\\\":\\\"LEGENDARY\\\",\\\"exp\\\":0.0}\"}"
```

Pet rarity tiers (filename suffix):
- `;0` = COMMON
- `;1` = UNCOMMON
- `;2` = RARE
- `;3` = EPIC
- `;4` = LEGENDARY

### Enchantment Fields

```json
"parent": "SHARPNESS;4",
"nbttag": "{ExtraAttributes:{enchantments:{sharpness:5}}}"
```

### NPC Location Fields

```json
"x": -42,
"y": 72,
"z": -65,
"island": "hub"
```

### Vanilla Items

```json
"vanilla": true
```

**Count:** 490 vanilla Minecraft items

### Parent/Child Relationships

```json
"parent": "ITEM;4"
```

**Count:** 473 items with parent reference

---

## Minecraft Item IDs

Top item IDs used:

| Item ID | Count | Use |
|---------|-------|-----|
| `minecraft:skull` | 4,778 | Custom heads, pets, NPCs |
| `minecraft:enchanted_book` | 698 | Enchantments |
| `minecraft:potion` | 229 | Potions |
| `minecraft:leather_boots` | 138 | Dyed armor |
| `minecraft:leather_leggings` | 133 | Dyed armor |
| `minecraft:leather_chestplate` | 129 | Dyed armor |
| `minecraft:iron_sword` | 47 | Swords |
| `minecraft:dye` | 47 | Dyes |
| `minecraft:stick` | 40 | Wands/tools |
| `minecraft:bow` | 38 | Bows |

---

## Island Locations

NPCs and items have island locations:

| Island | Code | Count |
|--------|------|-------|
| Hub | `hub` | 111 |
| Rift | `rift` | 101 |
| Crimson Isle | `crimson_isle` | 80 |
| Crystal Hollows | `mining_3` | 50 |
| Dark Thicket | `foraging_2` | 23 |
| The Barn | `farming_1` | 16 |
| Spider's Den | `combat_1` | 11 |
| Park | `foraging_1` | 10 |
| Jerry's Workshop | `winter` | 8 |
| Mineshaft | `mineshaft` | 5 |
| The End | `combat_3` | 5 |
| Gold Mine | `mining_1` | 4 |
| Dungeon Hub | `dungeon_hub` | 4 |
| Deep Caverns | `mining_2` | 3 |
| Mushroom Desert | `fishing_1` | 3 |
| Crystal Hollows | `crystal_hollows` | 3 |
| Garden | `garden` | 2 |

---

## Slayer Requirements

Items with slayer requirements:

| Slayer | Levels | Example Field |
|--------|--------|---------------|
| Zombie | 1-8 | `"slayer_req": "ZOMBIE_5"` |
| Spider | 1-8 | `"slayer_req": "SPIDER_7"` |
| Wolf | 1-7 | `"slayer_req": "WOLF_6"` |
| Enderman | 1-7 | `"slayer_req": "EMAN_4"` |
| Blaze | 1-7 | `"slayer_req": "BLAZE_6"` |

---

## Command Examples

```bash
# === RECIPE TYPES ===
# Crafting table items
grep -l '"recipe":' items/*.json | wc -l

# Forge items
grep -l '"type": "forge"' items/*.json

# NPC shop items
grep -l '"type": "npc_shop"' items/*.json

# Drop items
grep -l '"type": "drops"' items/*.json

# Kat upgrade items
grep -l '"type": "katgrade"' items/*.json

# Trade items
grep -l '"type": "trade"' items/*.json

# === CATEGORIES ===
# Pets
ls items/*\;[0-4].json

# NPCs
ls items/*_NPC.json

# Generators
ls items/*GENERATOR*.json

# Armor pieces
ls items/*_HELMET.json items/*_CHESTPLATE.json items/*_LEGGINGS.json items/*_BOOTS.json

# Enchantments
ls items/*\;[1-9].json items/*\;10.json

# Shards
ls items/*SHARD*.json

# === SPECIAL FIELDS ===
# Vanilla items
grep -l '"vanilla": true' items/*.json

# Items with locations
grep -l '"island":' items/*.json

# Items with parent
grep -l '"parent":' items/*.json

# === RARITY ===
# Legendary items
grep -l '§6§lLEGENDARY' items/*.json

# Mythic items
grep -l '§d§lMYTHIC' items/*.json

# === SLAYER ===
# All slayer items
grep -l '"slayer_req"' items/*.json

# Specific slayer
grep -l '"slayer_req": "EMAN' items/*.json

# === ACCESSORIES ===
grep -l 'ACCESSORY' items/*.json

# === COSMETICS ===
grep -l 'COSMETIC' items/*.json

# === DUNGEON ITEMS ===
grep -l 'DUNGEON' items/*.json
```

---

## Color Codes Reference

Minecraft color codes used in `displayname` and `lore`:

| Code | Color | Code | Format |
|------|-------|------|--------|
| `§0` | Black | `§l` | Bold |
| `§1` | Dark Blue | `§m` | Strikethrough |
| `§2` | Dark Green | `§n` | Underline |
| `§3` | Dark Aqua | `§o` | Italic |
| `§4` | Dark Red | `§r` | Reset |
| `§5` | Purple | `§k` | Obfuscated |
| `§6` | Gold | | |
| `§7` | Gray | | |
| `§8` | Dark Gray | | |
| `§9` | Blue | | |
| `§a` | Green | | |
| `§b` | Aqua | | |
| `§c` | Red | | |
| `§d` | Pink | | |
| `§e` | Yellow | | |
| `§f` | White | | |

---

## Stats Symbols

Common stat symbols in lore:

| Symbol | Stat |
|--------|------|
| `❤` | Health |
| `❈` | Defense |
| `❁` | Strength/Damage |
| `☠` | Crit Damage |
| `✎` | Intelligence |
| `✦` | Speed |
| `☘` | Fortune |
| `⸕` | Mining Speed |
| `✧` | Magic Find |
| `α` | Bonus Attack Speed |
| `⚔` | Combat |
| `⚓` | Fishing |


