import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import axios, { AxiosInstance } from 'axios';
import { Vector2ExportDto } from '../dto';

@Injectable()
export class CrmClientService {
  private readonly logger = new Logger(CrmClientService.name);
  private readonly client: AxiosInstance | null;
  private readonly apiUrl: string;

  constructor(private readonly configService: ConfigService) {
    this.apiUrl = this.configService.get<string>('CRM_API_URL', '');
    const accessKey = this.configService.get<string>('CRM_ACCESS_KEY', '');

    if (this.apiUrl) {
      this.client = axios.create({
        baseURL: this.apiUrl,
        timeout: 30000,
        headers: accessKey ? { 'X-Access-Key': accessKey } : {},
      });
      this.logger.log(`CRM client initialized: ${this.apiUrl}`);
    } else {
      this.client = null;
      this.logger.warn('CRM_API_URL not configured — export will run in dry-run mode');
    }
  }

  isConfigured(): boolean {
    return !!this.client;
  }

  async importObject(dto: Vector2ExportDto): Promise<{ success: boolean; id?: string; error?: string }> {
    if (!this.client) {
      this.logger.debug(`[dry-run] Would import: external_id=${dto.external_id}, platform=${dto.source_platform}`);
      return { success: true, id: `dry-run-${dto.external_id}` };
    }

    // Retry once on timeout (object may already be created on CRM side)
    for (let attempt = 1; attempt <= 2; attempt++) {
      try {
        const response = await this.client.post('/import-object', dto);
        // CRM response: {"success":true,"data":{"id":396974,"external_id":...}}
        const payload = response.data?.data || response.data;
        return {
          success: true,
          id: String(payload.id || payload.external_id || dto.external_id),
        };
      } catch (error) {
        const isTimeout = axios.isAxiosError(error) && error.code === 'ECONNABORTED';
        if (isTimeout && attempt === 1) {
          this.logger.warn(`CRM timeout for external_id=${dto.external_id}, retrying...`);
          await new Promise(r => setTimeout(r, 2000));
          continue;
        }
        const message = axios.isAxiosError(error)
          ? `${error.response?.status} ${JSON.stringify(error.response?.data || error.message)}`
          : String(error);
        this.logger.warn(`CRM import failed for external_id=${dto.external_id}: ${message}`);
        return { success: false, error: message };
      }
    }

    return { success: false, error: 'Unexpected retry exit' };
  }

  /**
   * Archive (deactivate) an object in CRM via the same /import-object endpoint.
   * CRM checks `deleted_at` field — if present, calls handleArchive() internally.
   * Sets archive_reason = 9 (Інше), archive_reason_text = "Імпорт: знятий з зовнішнього джерела".
   * @param externalId - source external_id (aggregator ID)
   */
  async archiveObject(
    externalId: string | number,
  ): Promise<{ success: boolean; error?: string }> {
    if (!this.client) {
      this.logger.debug(`[dry-run] Would archive CRM object: external_id=${externalId}`);
      return { success: true };
    }

    try {
      const response = await this.client.post('/import-object', {
        external_id: String(externalId),
        deleted_at: new Date().toISOString(),
      });
      const data = response.data?.data || response.data;
      if (data?.archived) {
        return { success: true };
      }
      return { success: true }; // CRM may return success differently
    } catch (error) {
      const message = axios.isAxiosError(error)
        ? `${error.response?.status} ${JSON.stringify(error.response?.data || error.message)}`
        : String(error);
      this.logger.warn(`CRM archive failed for external_id=${externalId}: ${message}`);
      return { success: false, error: message };
    }
  }
}
