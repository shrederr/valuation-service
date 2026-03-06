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
    const apiKey = this.configService.get<string>('CRM_API_KEY', '');

    if (this.apiUrl) {
      this.client = axios.create({
        baseURL: this.apiUrl,
        timeout: 10000,
        headers: apiKey ? { Authorization: `Bearer ${apiKey}` } : {},
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

  async createObject(dto: Vector2ExportDto): Promise<{ id: string }> {
    if (!this.client) {
      this.logger.debug(`[dry-run] Would create: source_id=${dto.source_id}, platform=${dto.source_platform}`);
      return { id: `dry-run-${dto.source_id}` };
    }

    const response = await this.client.post('/objects', dto);
    return { id: String(response.data.id || response.data.externalId) };
  }

  async updateObject(crmId: string, dto: Vector2ExportDto): Promise<void> {
    if (!this.client) {
      this.logger.debug(`[dry-run] Would update: crmId=${crmId}`);
      return;
    }

    await this.client.put(`/objects/${crmId}`, dto);
  }

  async deactivateObject(crmId: string): Promise<void> {
    if (!this.client) {
      this.logger.debug(`[dry-run] Would deactivate: crmId=${crmId}`);
      return;
    }

    await this.client.patch(`/objects/${crmId}`, { is_active: false });
  }
}
