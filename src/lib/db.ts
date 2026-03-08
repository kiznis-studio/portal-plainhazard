// PlainHazard Database Query Layer
// FEMA OpenFEMA + NOAA Storm Events Data

import type { D1Database } from './d1-adapter';

const queryCache = new Map<string, any>();
export function getQueryCacheSize(): number { return queryCache.size; }
function cached<T>(key: string, compute: () => Promise<T>): Promise<T> {
  if (queryCache.has(key)) return Promise.resolve(queryCache.get(key) as T);
  return compute().then(result => { queryCache.set(key, result); return result; });
}

// --- Interfaces ---

export interface State {
  code: string;
  name: string;
  slug: string;
  total_disasters: number;
  major_disasters: number;
  emergencies: number;
  fire_mgmt: number;
  first_disaster_year: number;
  latest_disaster_year: number;
  top_incident_type: string;
  total_storm_events: number;
  total_fatalities: number;
  total_injuries: number;
  total_property_damage: number;
  total_crop_damage: number;
}

export interface Disaster {
  disaster_number: number;
  state: string;
  declaration_type: string;
  title: string;
  fiscal_year: number;
  incident_type: string;
  declaration_date: string;
  incident_begin: string;
  incident_end: string;
  fips_state: string;
  fips_county: string;
  designated_area: string;
}

export interface County {
  fips: string;
  name: string;
  slug: string;
  state_code: string;
  state_name: string;
  total_disasters: number;
  major_disasters: number;
  top_incident_type: string;
  first_disaster_year: number;
  latest_disaster_year: number;
}

export interface StormEvent {
  state: string;
  year: number;
  event_type: string;
  events: number;
  fatalities: number;
  injuries: number;
  property_damage: number;
  crop_damage: number;
}

export interface StormEventType {
  event_type: string;
  slug: string;
  total_events: number;
  total_fatalities: number;
  total_injuries: number;
  total_property_damage: number;
  total_crop_damage: number;
  states_affected: number;
}

export interface DisasterByType {
  incident_type: string;
  slug: string;
  total_count: number;
  state_count: number;
  earliest_year: number;
  latest_year: number;
}

export interface DisasterByYear {
  year: number;
  total_count: number;
  major_disasters: number;
  emergencies: number;
}

export interface StateDisasterType {
  state_code: string;
  incident_type: string;
  count: number;
}

export interface StateStormSummary {
  state_code: string;
  year: number;
  total_events: number;
  total_fatalities: number;
  total_injuries: number;
  total_property_damage: number;
  total_crop_damage: number;
}

export interface Meta {
  key: string;
  value: string;
}

export interface Stats {
  total_states: number;
  total_counties: number;
  total_disasters: number;
  total_storm_events: number;
  total_fatalities: number;
  total_property_damage: number;
  total_storm_types: number;
  total_disaster_types: number;
}

// --- Format helpers ---

export function fmtDamage(n: number | null | undefined): string {
  if (n == null || isNaN(n) || n === 0) return '$0';
  const abs = Math.abs(n);
  if (abs >= 1_000_000_000) return `$${(n / 1_000_000_000).toFixed(1)}B`;
  if (abs >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `$${(n / 1_000).toFixed(1)}K`;
  return `$${Math.round(n).toLocaleString()}`;
}

export function fmtNumber(n: number | null | undefined): string {
  if (n == null || isNaN(n)) return '0';
  return n.toLocaleString();
}

export function fmtPct(part: number, total: number): string {
  if (!total) return '0%';
  return `${((part / total) * 100).toFixed(1)}%`;
}

// --- Query functions ---

export function getStats(db: D1Database): Promise<Stats> {
  return cached('stats', async () => {
    const states = await db.prepare('SELECT COUNT(*) as c FROM states').first<{ c: number }>();
    const counties = await db.prepare('SELECT COUNT(*) as c FROM counties').first<{ c: number }>();
    const disasters = await db.prepare('SELECT SUM(total_disasters) as c FROM states').first<{ c: number }>();
    const stormEvents = await db.prepare('SELECT SUM(total_storm_events) as c FROM states').first<{ c: number }>();
    const fatalities = await db.prepare('SELECT SUM(total_fatalities) as c FROM states').first<{ c: number }>();
    const damage = await db.prepare('SELECT SUM(total_property_damage) as c FROM states').first<{ c: number }>();
    const stormTypes = await db.prepare('SELECT COUNT(*) as c FROM storm_event_types').first<{ c: number }>();
    const disasterTypes = await db.prepare('SELECT COUNT(*) as c FROM disaster_by_type').first<{ c: number }>();

    return {
      total_states: states?.c || 0,
      total_counties: counties?.c || 0,
      total_disasters: disasters?.c || 0,
      total_storm_events: stormEvents?.c || 0,
      total_fatalities: fatalities?.c || 0,
      total_property_damage: damage?.c || 0,
      total_storm_types: stormTypes?.c || 0,
      total_disaster_types: disasterTypes?.c || 0,
    };
  });
}

// States
export function getStates(db: D1Database): Promise<State[]> {
  return cached('all-states', async () => {
    const { results } = await db.prepare(
      'SELECT * FROM states ORDER BY total_disasters DESC'
    ).all<State>();
    return results;
  });
}

export function getStatesByName(db: D1Database): Promise<State[]> {
  return cached('states-by-name', async () => {
    const { results } = await db.prepare(
      'SELECT * FROM states ORDER BY name COLLATE NOCASE ASC'
    ).all<State>();
    return results;
  });
}

export async function getState(db: D1Database, slug: string): Promise<State | null> {
  return db.prepare('SELECT * FROM states WHERE slug = ?').bind(slug).first<State>();
}

export function getTopStatesByDisasters(db: D1Database, limit = 10): Promise<State[]> {
  return cached(`top-states-disasters:${limit}`, async () => {
    const { results } = await db.prepare(
      'SELECT * FROM states ORDER BY total_disasters DESC LIMIT ?'
    ).bind(limit).all<State>();
    return results;
  });
}

export function getTopStatesByStormEvents(db: D1Database, limit = 10): Promise<State[]> {
  return cached(`top-states-storms:${limit}`, async () => {
    const { results } = await db.prepare(
      'SELECT * FROM states ORDER BY total_storm_events DESC LIMIT ?'
    ).bind(limit).all<State>();
    return results;
  });
}

export function getTopStatesByDamage(db: D1Database, limit = 10): Promise<State[]> {
  return cached(`top-states-damage:${limit}`, async () => {
    const { results } = await db.prepare(
      'SELECT * FROM states ORDER BY (total_property_damage + total_crop_damage) DESC LIMIT ?'
    ).bind(limit).all<State>();
    return results;
  });
}

export function getTopStatesByFatalities(db: D1Database, limit = 10): Promise<State[]> {
  return cached(`top-states-fatalities:${limit}`, async () => {
    const { results } = await db.prepare(
      'SELECT * FROM states ORDER BY total_fatalities DESC LIMIT ?'
    ).bind(limit).all<State>();
    return results;
  });
}

// Counties
export async function getCountiesByState(db: D1Database, stateCode: string): Promise<County[]> {
  const { results } = await db.prepare(
    'SELECT * FROM counties WHERE state_code = ? ORDER BY total_disasters DESC'
  ).bind(stateCode).all<County>();
  return results;
}

export async function getCounty(db: D1Database, slug: string): Promise<County | null> {
  return db.prepare('SELECT * FROM counties WHERE slug = ?').bind(slug).first<County>();
}

export function getTopCounties(db: D1Database, limit = 50): Promise<County[]> {
  return cached(`top-counties:${limit}`, async () => {
    const { results } = await db.prepare(
      'SELECT * FROM counties ORDER BY total_disasters DESC LIMIT ?'
    ).bind(limit).all<County>();
    return results;
  });
}

export function getAllCounties(db: D1Database): Promise<County[]> {
  return cached('all-counties', async () => {
    const { results } = await db.prepare(
      'SELECT * FROM counties ORDER BY total_disasters DESC'
    ).all<County>();
    return results;
  });
}

// Disasters
export async function getDisastersByState(db: D1Database, stateCode: string): Promise<Disaster[]> {
  const { results } = await db.prepare(
    'SELECT DISTINCT disaster_number, state, declaration_type, title, fiscal_year, incident_type, declaration_date, incident_begin, incident_end FROM disasters WHERE state = ? ORDER BY declaration_date DESC'
  ).bind(stateCode).all<Disaster>();
  return results;
}

export async function getDisastersByCounty(db: D1Database, fips: string): Promise<Disaster[]> {
  const fipsState = fips.substring(0, 2);
  const fipsCounty = fips.substring(2);
  const { results } = await db.prepare(
    'SELECT * FROM disasters WHERE fips_state = ? AND fips_county = ? ORDER BY declaration_date DESC'
  ).bind(fipsState, fipsCounty).all<Disaster>();
  return results;
}

export async function getDisastersByType(db: D1Database, incidentType: string): Promise<Disaster[]> {
  const { results } = await db.prepare(
    'SELECT DISTINCT disaster_number, state, declaration_type, title, fiscal_year, incident_type, declaration_date, incident_begin, incident_end FROM disasters WHERE incident_type = ? ORDER BY declaration_date DESC LIMIT 100'
  ).bind(incidentType).all<Disaster>();
  return results;
}

// Storm Events
export async function getStormEventsByState(db: D1Database, stateCode: string): Promise<StormEvent[]> {
  const { results } = await db.prepare(
    'SELECT * FROM storm_events WHERE state = ? ORDER BY year DESC, events DESC'
  ).bind(stateCode).all<StormEvent>();
  return results;
}

export async function getStormEventsByType(db: D1Database, eventType: string): Promise<StormEvent[]> {
  const { results } = await db.prepare(
    'SELECT * FROM storm_events WHERE event_type = ? ORDER BY events DESC'
  ).bind(eventType).all<StormEvent>();
  return results;
}

// Storm Event Types
export function getStormEventTypes(db: D1Database): Promise<StormEventType[]> {
  return cached('all-storm-types', async () => {
    const { results } = await db.prepare(
      'SELECT * FROM storm_event_types ORDER BY total_events DESC'
    ).all<StormEventType>();
    return results;
  });
}

export async function getStormEventType(db: D1Database, slug: string): Promise<StormEventType | null> {
  return db.prepare('SELECT * FROM storm_event_types WHERE slug = ?').bind(slug).first<StormEventType>();
}

// Disaster Types
export function getDisasterTypes(db: D1Database): Promise<DisasterByType[]> {
  return cached('all-disaster-types', async () => {
    const { results } = await db.prepare(
      'SELECT * FROM disaster_by_type ORDER BY total_count DESC'
    ).all<DisasterByType>();
    return results;
  });
}

export async function getDisasterType(db: D1Database, slug: string): Promise<DisasterByType | null> {
  return db.prepare('SELECT * FROM disaster_by_type WHERE slug = ?').bind(slug).first<DisasterByType>();
}

// Disaster by Year
export function getDisastersByYear(db: D1Database): Promise<DisasterByYear[]> {
  return cached('disasters-by-year', async () => {
    const { results } = await db.prepare(
      'SELECT * FROM disaster_by_year ORDER BY year DESC'
    ).all<DisasterByYear>();
    return results;
  });
}

// State disaster types
export async function getStateDisasterTypes(db: D1Database, stateCode: string): Promise<StateDisasterType[]> {
  const { results } = await db.prepare(
    'SELECT * FROM state_disaster_types WHERE state_code = ? ORDER BY count DESC'
  ).bind(stateCode).all<StateDisasterType>();
  return results;
}

// State storm summary (by year)
export async function getStateStormSummary(db: D1Database, stateCode: string): Promise<StateStormSummary[]> {
  const { results } = await db.prepare(
    'SELECT * FROM state_storm_summary WHERE state_code = ? ORDER BY year DESC'
  ).bind(stateCode).all<StateStormSummary>();
  return results;
}

// Search
export async function search(db: D1Database, query: string, limit = 10): Promise<{
  states: State[];
  counties: County[];
  hazards: StormEventType[];
}> {
  const q = '%' + query + '%';
  const { results: states } = await db.prepare(
    'SELECT * FROM states WHERE name LIKE ? OR code LIKE ? ORDER BY total_disasters DESC LIMIT ?'
  ).bind(q, q, limit).all<State>();
  const { results: counties } = await db.prepare(
    'SELECT * FROM counties WHERE name LIKE ? OR state_name LIKE ? ORDER BY total_disasters DESC LIMIT ?'
  ).bind(q, q, limit).all<County>();
  const { results: hazards } = await db.prepare(
    'SELECT * FROM storm_event_types WHERE event_type LIKE ? ORDER BY total_events DESC LIMIT ?'
  ).bind(q, 5).all<StormEventType>();
  return { states, counties, hazards };
}

// Meta
export async function getMeta(db: D1Database): Promise<Record<string, string>> {
  const { results } = await db.prepare('SELECT * FROM meta').all<Meta>();
  const map: Record<string, string> = {};
  for (const row of results) map[row.key] = row.value;
  return map;
}

// Slugs for sitemaps
export function getAllStateSlugs(db: D1Database): Promise<{ slug: string }[]> {
  return cached('slugs-states', async () => {
    const { results } = await db.prepare('SELECT slug FROM states ORDER BY total_disasters DESC').all<{ slug: string }>();
    return results;
  });
}

export function getAllCountySlugs(db: D1Database): Promise<{ slug: string }[]> {
  return cached('slugs-counties', async () => {
    const { results } = await db.prepare('SELECT slug FROM counties ORDER BY total_disasters DESC').all<{ slug: string }>();
    return results;
  });
}

export function getAllStormTypeSlugs(db: D1Database): Promise<{ slug: string }[]> {
  return cached('slugs-storm-types', async () => {
    const { results } = await db.prepare('SELECT slug FROM storm_event_types ORDER BY total_events DESC').all<{ slug: string }>();
    return results;
  });
}

export function getAllDisasterTypeSlugs(db: D1Database): Promise<{ slug: string }[]> {
  return cached('slugs-disaster-types', async () => {
    const { results } = await db.prepare('SELECT slug FROM disaster_by_type ORDER BY total_count DESC').all<{ slug: string }>();
    return results;
  });
}

// Pre-warm all shared query caches at startup.
export async function warmQueryCache(db: D1Database): Promise<number> {
  const start = Date.now();
  await Promise.all([
    getStats(db),
    getStates(db),
    getStatesByName(db),
    getTopStatesByDisasters(db),
    getTopStatesByStormEvents(db),
    getTopStatesByDamage(db),
    getTopStatesByFatalities(db),
    getTopCounties(db),
    getAllCounties(db),
    getStormEventTypes(db),
    getDisasterTypes(db),
    getDisastersByYear(db),
    getAllStateSlugs(db),
    getAllCountySlugs(db),
    getAllStormTypeSlugs(db),
    getAllDisasterTypeSlugs(db),
  ]);
  console.log(`[cache] Warmed ${queryCache.size} queries in ${Date.now() - start}ms`);
  return queryCache.size;
}
