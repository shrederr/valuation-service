import { Injectable } from '@nestjs/common';
import { UnifiedListing } from '@libs/database';

export interface ScoredAnalog {
  listing: UnifiedListing;
  matchScore: number;
}

@Injectable()
export class AnalogScorerService {
  private readonly WEIGHTS = {
    area: 0.3,
    rooms: 0.2,
    floor: 0.15,
    condition: 0.15,
    houseType: 0.1,
    location: 0.1,
  };

  public scoreAnalogs(subject: UnifiedListing, candidates: UnifiedListing[]): ScoredAnalog[] {
    return candidates.map((candidate) => ({
      listing: candidate,
      matchScore: this.calculateMatchScore(subject, candidate),
    }));
  }

  private calculateMatchScore(subject: UnifiedListing, candidate: UnifiedListing): number {
    let score = 0;
    let totalWeight = 0;

    const areaScore = this.scoreArea(subject, candidate);
    if (areaScore !== null) {
      score += areaScore * this.WEIGHTS.area;
      totalWeight += this.WEIGHTS.area;
    }

    const roomsScore = this.scoreRooms(subject, candidate);
    if (roomsScore !== null) {
      score += roomsScore * this.WEIGHTS.rooms;
      totalWeight += this.WEIGHTS.rooms;
    }

    const floorScore = this.scoreFloor(subject, candidate);
    if (floorScore !== null) {
      score += floorScore * this.WEIGHTS.floor;
      totalWeight += this.WEIGHTS.floor;
    }

    const conditionScore = this.scoreCondition(subject, candidate);
    if (conditionScore !== null) {
      score += conditionScore * this.WEIGHTS.condition;
      totalWeight += this.WEIGHTS.condition;
    }

    const houseTypeScore = this.scoreHouseType(subject, candidate);
    if (houseTypeScore !== null) {
      score += houseTypeScore * this.WEIGHTS.houseType;
      totalWeight += this.WEIGHTS.houseType;
    }

    const locationScore = this.scoreLocation(subject, candidate);
    if (locationScore !== null) {
      score += locationScore * this.WEIGHTS.location;
      totalWeight += this.WEIGHTS.location;
    }

    if (totalWeight === 0) {
      return 0.5;
    }

    return Math.round((score / totalWeight) * 100) / 100;
  }

  private scoreArea(subject: UnifiedListing, candidate: UnifiedListing): number | null {
    if (!subject.totalArea || !candidate.totalArea) {
      return null;
    }

    const subjectArea = Number(subject.totalArea);
    const candidateArea = Number(candidate.totalArea);
    const diff = Math.abs(subjectArea - candidateArea);
    const percentDiff = diff / subjectArea;

    if (percentDiff <= 0.05) return 1;
    if (percentDiff <= 0.1) return 0.9;
    if (percentDiff <= 0.15) return 0.8;
    if (percentDiff <= 0.2) return 0.7;
    if (percentDiff <= 0.3) return 0.5;

    return 0.3;
  }

  private scoreRooms(subject: UnifiedListing, candidate: UnifiedListing): number | null {
    if (!subject.rooms || !candidate.rooms) {
      return null;
    }

    const diff = Math.abs(subject.rooms - candidate.rooms);

    if (diff === 0) return 1;
    if (diff === 1) return 0.7;
    if (diff === 2) return 0.4;

    return 0.2;
  }

  private scoreFloor(subject: UnifiedListing, candidate: UnifiedListing): number | null {
    if (!subject.floor || !candidate.floor) {
      return null;
    }

    const subjectFloorType = this.getFloorType(subject.floor, subject.totalFloors);
    const candidateFloorType = this.getFloorType(candidate.floor, candidate.totalFloors);

    if (subjectFloorType === candidateFloorType) {
      return 1;
    }

    if (
      (subjectFloorType === 'first' && candidateFloorType === 'middle') ||
      (subjectFloorType === 'middle' && candidateFloorType === 'first')
    ) {
      return 0.7;
    }

    if (
      (subjectFloorType === 'last' && candidateFloorType === 'middle') ||
      (subjectFloorType === 'middle' && candidateFloorType === 'last')
    ) {
      return 0.7;
    }

    return 0.5;
  }

  private getFloorType(floor: number, totalFloors?: number): 'first' | 'last' | 'middle' {
    if (floor === 1) {
      return 'first';
    }

    if (totalFloors && floor === totalFloors) {
      return 'last';
    }

    return 'middle';
  }

  private scoreCondition(subject: UnifiedListing, candidate: UnifiedListing): number | null {
    if (!subject.condition || !candidate.condition) {
      return null;
    }

    if (subject.condition === candidate.condition) {
      return 1;
    }

    const conditionRanks: Record<string, number> = {
      'без ремонту': 1,
      'потребує ремонту': 1,
      косметичний: 2,
      житловий: 2,
      євроремонт: 3,
      дизайнерський: 4,
    };

    const subjectRank = conditionRanks[subject.condition.toLowerCase()] || 2;
    const candidateRank = conditionRanks[candidate.condition.toLowerCase()] || 2;
    const diff = Math.abs(subjectRank - candidateRank);

    if (diff === 0) return 1;
    if (diff === 1) return 0.7;
    if (diff === 2) return 0.4;

    return 0.2;
  }

  private scoreHouseType(subject: UnifiedListing, candidate: UnifiedListing): number | null {
    if (!subject.houseType || !candidate.houseType) {
      return null;
    }

    if (subject.houseType === candidate.houseType) {
      return 1;
    }

    const similarTypes: Record<string, string[]> = {
      панель: ['панель', 'блок'],
      блок: ['блок', 'панель'],
      цегла: ['цегла', 'моноліт'],
      моноліт: ['моноліт', 'цегла', 'монолітно-каркасний'],
      'монолітно-каркасний': ['монолітно-каркасний', 'моноліт'],
    };

    const subjectSimilar = similarTypes[subject.houseType.toLowerCase()] || [];

    if (subjectSimilar.includes(candidate.houseType.toLowerCase())) {
      return 0.8;
    }

    return 0.4;
  }

  private scoreLocation(subject: UnifiedListing, candidate: UnifiedListing): number | null {
    if (subject.complexId && candidate.complexId && subject.complexId === candidate.complexId) {
      return 1;
    }

    if (subject.streetId && candidate.streetId && subject.streetId === candidate.streetId) {
      if (subject.houseNumber === candidate.houseNumber) {
        return 0.95;
      }

      return 0.85;
    }

    if (subject.topzoneId && candidate.topzoneId && subject.topzoneId === candidate.topzoneId) {
      return 0.7;
    }

    if (subject.geoId && candidate.geoId && subject.geoId === candidate.geoId) {
      return 0.5;
    }

    return 0.3;
  }
}
