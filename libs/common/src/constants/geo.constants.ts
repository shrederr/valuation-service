import { GeoType } from '../enums';

export const WGS84_SRID = 4326;

export const OSM_ADMIN_LEVEL_MAP: Record<number, GeoType> = {
  2: GeoType.Country,
  4: GeoType.Region,
  6: GeoType.RegionDistrict,
  8: GeoType.City,
  9: GeoType.CityDistrict,
  10: GeoType.CityDistrict,
};

export const OSM_VILLAGE_PLACE_TAGS = ['village', 'hamlet', 'isolated_dwelling', 'farm', 'allotments'];

export const OSM_BUFFER_RADIUS: Record<string, number> = {
  [GeoType.City]: 2000,
  [GeoType.Village]: 500,
};

export const DEFAULT_OVERPASS_URL = 'https://overpass-api.de/api/interpreter';
