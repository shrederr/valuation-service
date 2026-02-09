import { Injectable } from '@nestjs/common';
import { UnifiedListing } from '@libs/database';

export interface ScoredAnalog {
  listing: UnifiedListing;
  matchScore: number;
}

@Injectable()
export class AnalogScorerService {
  private readonly WEIGHTS = {
    area: 0.25,
    rooms: 0.18,
    floor: 0.12,
    condition: 0.12,
    houseType: 0.08,
    location: 0.1,
    infrastructure: 0.15,
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

    const infrastructureScore = this.scoreInfrastructure(subject, candidate);
    if (infrastructureScore !== null) {
      score += infrastructureScore * this.WEIGHTS.infrastructure;
      totalWeight += this.WEIGHTS.infrastructure;
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

  /**
   * Score infrastructure similarity between subject and candidate.
   * Compares distances to public transport, schools, hospitals, and supermarkets.
   * Higher scores when both have similar infrastructure access.
   */
  private scoreInfrastructure(subject: UnifiedListing, candidate: UnifiedListing): number | null {
    // Check if either has infrastructure data
    const subjectHasData = subject.nearestPublicTransport || subject.nearestSchool ||
                           subject.nearestHospital || subject.nearestSupermarket;
    const candidateHasData = candidate.nearestPublicTransport || candidate.nearestSchool ||
                             candidate.nearestHospital || candidate.nearestSupermarket;

    if (!subjectHasData && !candidateHasData) {
      return null;
    }

    // If only one has data, give partial score
    if (!subjectHasData || !candidateHasData) {
      return 0.5;
    }

    let totalScore = 0;
    let count = 0;

    // Score public transport (weight: 40%)
    const transportScore = this.scoreDistanceCategory(
      subject.nearestPublicTransport,
      candidate.nearestPublicTransport,
    );
    if (transportScore !== null) {
      totalScore += transportScore * 0.4;
      count += 0.4;
    }

    // Score school proximity (weight: 25%)
    const schoolScore = this.scoreDistanceCategory(
      subject.nearestSchool,
      candidate.nearestSchool,
    );
    if (schoolScore !== null) {
      totalScore += schoolScore * 0.25;
      count += 0.25;
    }

    // Score supermarket proximity (weight: 20%)
    const supermarketScore = this.scoreDistanceCategory(
      subject.nearestSupermarket,
      candidate.nearestSupermarket,
    );
    if (supermarketScore !== null) {
      totalScore += supermarketScore * 0.2;
      count += 0.2;
    }

    // Score hospital proximity (weight: 15%)
    const hospitalScore = this.scoreDistanceCategory(
      subject.nearestHospital,
      candidate.nearestHospital,
    );
    if (hospitalScore !== null) {
      totalScore += hospitalScore * 0.15;
      count += 0.15;
    }

    if (count === 0) {
      return null;
    }

    return totalScore / count;
  }

  /**
   * Compare two distances and return a similarity score.
   * Both distances are categorized into bands, and we score based on same/similar band.
   */
  private scoreDistanceCategory(
    subjectDist: number | undefined | null,
    candidateDist: number | undefined | null,
  ): number | null {
    if (subjectDist == null || candidateDist == null) {
      return null;
    }

    const subjectCategory = this.getDistanceCategory(subjectDist);
    const candidateCategory = this.getDistanceCategory(candidateDist);

    // Same category = perfect match
    if (subjectCategory === candidateCategory) {
      return 1;
    }

    // Adjacent category = good match
    const diff = Math.abs(subjectCategory - candidateCategory);
    if (diff === 1) {
      return 0.7;
    }
    if (diff === 2) {
      return 0.4;
    }

    return 0.2;
  }

  /**
   * Categorize distance into bands:
   * 1 = Very close (0-300m)
   * 2 = Close (300-500m)
   * 3 = Walking distance (500-800m)
   * 4 = Medium (800-1200m)
   * 5 = Far (>1200m)
   */
  private getDistanceCategory(distance: number): number {
    if (distance <= 300) return 1;
    if (distance <= 500) return 2;
    if (distance <= 800) return 3;
    if (distance <= 1200) return 4;
    return 5;
  }
}
