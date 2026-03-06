import { Injectable } from '@nestjs/common';
import { UnifiedListing } from '@libs/database';

export interface ExtractedPrimaryData {
  phones: string[] | null;
  photos: string[] | null;
  description: string | null;
  url: string | null;
}

@Injectable()
export class PrimaryDataExtractor {
  extractForExport(listing: UnifiedListing): ExtractedPrimaryData {
    const attrs = listing.attributes || {};

    return {
      phones: this.extractPhones(attrs),
      photos: this.extractPhotos(attrs),
      description: this.extractDescription(listing),
      url: listing.externalUrl || this.extractUrl(attrs) || null,
    };
  }

  extractNormalizedPhone(listing: UnifiedListing): string | null {
    const attrs = listing.attributes || {};
    const phones = this.extractPhones(attrs);
    if (!phones || phones.length === 0) return null;
    return this.normalizePhone(phones[0]);
  }

  normalizePhone(phone: string): string {
    const digits = phone.replace(/\D/g, '');
    if (digits.startsWith('0') && digits.length === 10) {
      return '38' + digits;
    }
    if (digits.startsWith('8') && digits.length === 10) {
      return '38' + digits;
    }
    return digits;
  }

  private extractPhones(attrs: Record<string, unknown>): string[] | null {
    const phoneFields = ['phones', 'phone', 'sellerPhones', 'contactPhones'];
    for (const field of phoneFields) {
      const value = attrs[field];
      if (Array.isArray(value) && value.length > 0) {
        return value.map(String);
      }
      if (typeof value === 'string' && value.trim()) {
        return [value];
      }
    }

    const seller = attrs['seller'] as Record<string, unknown> | undefined;
    if (seller) {
      if (Array.isArray(seller['phones']) && seller['phones'].length > 0) {
        return seller['phones'].map(String);
      }
      if (typeof seller['phone'] === 'string' && seller['phone'].trim()) {
        return [seller['phone']];
      }
    }

    return null;
  }

  private extractPhotos(attrs: Record<string, unknown>): string[] | null {
    const photoFields = ['photos', 'images', 'photo', 'imageUrls'];
    for (const field of photoFields) {
      const value = attrs[field];
      if (Array.isArray(value) && value.length > 0) {
        return value.map(String);
      }
    }
    return null;
  }

  private extractDescription(listing: UnifiedListing): string | null {
    if (listing.description) {
      return listing.description.uk || listing.description.ru || listing.description.en || null;
    }

    const attrs = listing.attributes || {};
    const descFields = ['description', 'description_uk', 'descriptionUk'];
    for (const field of descFields) {
      const value = attrs[field];
      if (typeof value === 'string' && value.trim()) {
        return value;
      }
    }

    return null;
  }

  private extractUrl(attrs: Record<string, unknown>): string | null {
    const urlFields = ['url', 'externalUrl', 'beautifulUrl', 'sourceUrl'];
    for (const field of urlFields) {
      const value = attrs[field];
      if (typeof value === 'string' && value.trim()) {
        return value;
      }
    }
    return null;
  }
}
