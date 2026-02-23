import { Injectable } from '@nestjs/common';
import { UnifiedListing } from '@libs/database';

@Injectable()
export class AnalogFilterService {
  public filterCandidates(
    subject: UnifiedListing,
    candidates: UnifiedListing[],
    alreadyCollected: UnifiedListing[],
  ): UnifiedListing[] {
    const collectedIds = new Set(alreadyCollected.map((a) => a.id));

    return candidates.filter((candidate) => {
      if (collectedIds.has(candidate.id)) {
        return false;
      }

      if (this.isDuplicate(subject, candidate)) {
        return false;
      }

      if (!this.isAreaMatch(subject, candidate)) {
        return false;
      }

      if (!this.isRoomsMatch(subject, candidate)) {
        return false;
      }

      return true;
    });
  }

  private isDuplicate(source: UnifiedListing, candidate: UnifiedListing): boolean {
    if (source.sourceType === candidate.sourceType && source.sourceId === candidate.sourceId) {
      return true;
    }

    const sameBuilding = source.streetId === candidate.streetId && source.houseNumber === candidate.houseNumber;

    const sameUnit =
      source.apartmentNumber === candidate.apartmentNumber || (!source.apartmentNumber && !candidate.apartmentNumber);

    const sameArea =
      source.totalArea &&
      candidate.totalArea &&
      Math.abs(Number(source.totalArea) - Number(candidate.totalArea)) <= 2;

    const sameRooms = source.rooms === candidate.rooms;

    const samePrice =
      source.price &&
      candidate.price &&
      Math.abs(Number(source.price) - Number(candidate.price)) / Number(source.price) <= 0.05;

    if (sameBuilding && sameUnit && sameArea && sameRooms && samePrice) {
      return true;
    }

    return false;
  }

  private isAreaMatch(subject: UnifiedListing, candidate: UnifiedListing): boolean {
    if (!subject.totalArea) {
      return true; // Subject has no area info, skip filter
    }

    if (!candidate.totalArea) {
      return false; // Subject has area but candidate doesn't — reject
    }

    const subjectArea = Number(subject.totalArea);
    const candidateArea = Number(candidate.totalArea);

    const tolerance = this.getAreaTolerance(subjectArea);
    const diff = Math.abs(subjectArea - candidateArea);

    return diff <= tolerance;
  }

  private getAreaTolerance(area: number): number {
    if (area <= 40) {
      return 5;
    }

    if (area <= 100) {
      return 10;
    }

    return 25;
  }

  private isRoomsMatch(subject: UnifiedListing, candidate: UnifiedListing): boolean {
    if (!subject.rooms) {
      return true; // Subject has no rooms info (e.g. land plot), skip filter
    }

    if (!candidate.rooms) {
      return false; // Subject has rooms but candidate doesn't — reject
    }

    const diff = Math.abs(subject.rooms - candidate.rooms);

    if (subject.rooms <= 2) {
      return diff <= 1;
    }

    return diff <= 2;
  }
}
