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
    const pd = (listing as any).primaryData || {};

    return {
      phones: this.extractPhones(attrs) || this.extractPhones(pd),
      photos: this.extractPhotos(attrs) || this.extractPhotos(pd),
      description: this.extractDescription(listing),
      url: listing.externalUrl || this.extractUrl(attrs) || this.extractUrl(pd) || null,
    };
  }

  extractNormalizedPhone(listing: UnifiedListing): string | null {
    const attrs = listing.attributes || {};
    const pd = (listing as any).primaryData || {};
    const phones = this.extractPhones(attrs) || this.extractPhones(pd);
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
    const phoneFields = ['phones', 'phone', 'sellerPhones', 'contactPhones', 'rieltor_phones'];
    for (const field of phoneFields) {
      const value = attrs[field];
      if (Array.isArray(value) && value.length > 0) {
        const valid = value.map(String).filter(p => p.replace(/\D/g, '').length >= 7);
        if (valid.length > 0) return valid;
      }
      if (typeof value === 'string' && value.replace(/\D/g, '').length >= 7) {
        return [value];
      }
    }

    // OLX: user.phone or contact.phone; domRia: seller.phones
    for (const key of ['seller', 'user', 'contact']) {
      const nested = attrs[key] as Record<string, unknown> | undefined;
      if (!nested) continue;
      if (Array.isArray(nested['phones']) && nested['phones'].length > 0) {
        const valid = nested['phones'].map(String).filter(p => p.replace(/\D/g, '').length >= 7);
        if (valid.length > 0) return valid;
      }
      const phone = nested['phone'];
      if (typeof phone === 'string' && phone.replace(/\D/g, '').length >= 7) {
        return [phone];
      }
    }

    return null;
  }

  private extractPhotos(attrs: Record<string, unknown>): string[] | null {
    const photoFields = ['photos', 'images', 'photo', 'imageUrls', 'image_list', 'ad_img', 'photosSet'];
    for (const field of photoFields) {
      const value = attrs[field];
      if (Array.isArray(value) && value.length > 0) {
        return value.map(String);
      }
      // DomRia: photos is an object { "318499720": "dom/photo/...", ... } — extract values
      if (value && typeof value === 'object' && !Array.isArray(value)) {
        const urls = Object.values(value).filter(v => typeof v === 'string' && v.length > 5);
        if (urls.length > 0) {
          // DomRia photo paths need CDN prefix
          return (urls as string[]).map(u =>
            u.startsWith('http') ? u : `https://cdn.riastatic.com/photosnew/${u}`,
          );
        }
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
    const urlFields = ['url', 'externalUrl', 'beautifulUrl', 'sourceUrl', 'ad_link'];
    for (const field of urlFields) {
      const value = attrs[field];
      if (typeof value === 'string' && value.trim()) {
        return value;
      }
    }
    return null;
  }
}
