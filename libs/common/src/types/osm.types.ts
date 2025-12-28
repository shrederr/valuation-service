import { GeoType } from '../enums';

export type OsmRelation = {
  type: 'relation';
  id: number;
  tags: {
    name?: string;
    'name:uk'?: string;
    'name:ru'?: string;
    'name:en'?: string;
    admin_level?: string;
    place?: string;
    population?: string;
    wikidata?: string;
    koatuu?: string;
    'ref:koatuu'?: string;
    katottg?: string;
    'ref:UA:katottg'?: string;
  };
  members?: Array<{
    type: string;
    ref: number;
    role: string;
  }>;
};

export type OsmNodeWithTags = {
  type: 'node';
  id: number;
  lat: number;
  lon: number;
  tags?: {
    name?: string;
    'name:uk'?: string;
    'name:ru'?: string;
    'name:en'?: string;
    place?: string;
    population?: string;
    koatuu?: string;
    'ref:koatuu'?: string;
  };
};

export type OsmWayWithTags = {
  type: 'way';
  id: number;
  nodes: number[];
  tags?: {
    name?: string;
    'name:uk'?: string;
    'name:ru'?: string;
    'name:en'?: string;
    place?: string;
    population?: string;
    koatuu?: string;
    'ref:koatuu'?: string;
  };
};

export type OsmNode = {
  type: 'node';
  id: number;
  lat: number;
  lon: number;
};

export type OsmWay = {
  type: 'way';
  id: number;
  nodes: number[];
};

export type OverpassResponse = {
  elements: (OsmRelation | OsmNode | OsmNodeWithTags | OsmWay)[];
};

export type ParsedGeoData = {
  osmId: string;
  name: { uk?: string; ru?: string; en?: string };
  type: GeoType;
  adminLevel: number;
  population?: number;
  lat?: number;
  lng?: number;
  polygonWkt?: string;
};

export type NodeCoords = { lat: number; lon: number };
