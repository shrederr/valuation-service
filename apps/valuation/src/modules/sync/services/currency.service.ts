import { Injectable, Logger, OnModuleInit } from '@nestjs/common';

interface NbuRate {
  r030: number;
  txt: string;
  rate: number;
  cc: string;
  exchangedate: string;
}

export interface CurrencyRates {
  USD: number; // UAH per 1 USD
  EUR: number; // UAH per 1 EUR
}

const NBU_API = 'https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?json';
const DEFAULT_RATES: CurrencyRates = { USD: 41.0, EUR: 45.0 };
const REFRESH_INTERVAL = 60 * 60 * 1000; // 1 hour

@Injectable()
export class CurrencyService implements OnModuleInit {
  private readonly logger = new Logger(CurrencyService.name);
  private rates: CurrencyRates = DEFAULT_RATES;

  async onModuleInit(): Promise<void> {
    await this.refreshRates();
    setInterval(() => this.refreshRates(), REFRESH_INTERVAL);
  }

  getRates(): CurrencyRates {
    return this.rates;
  }

  /**
   * Convert price to USD
   * @param price — price value (already in real units, e.g. after ×1000)
   * @param currency — 'USD' | 'EUR' | 'UAH'
   */
  toUsd(price: number, currency: string): number {
    if (!price) return price;
    const cur = currency?.toUpperCase() || 'USD';
    if (cur === 'USD') return price;
    if (cur === 'UAH') return Math.round(price / this.rates.USD);
    if (cur === 'EUR') return Math.round((price * this.rates.EUR) / this.rates.USD);
    return price;
  }

  async refreshRates(): Promise<void> {
    try {
      const response = await fetch(NBU_API);
      const data: NbuRate[] = await response.json();

      const usd = data.find((r) => r.cc === 'USD');
      const eur = data.find((r) => r.cc === 'EUR');

      if (usd && eur) {
        this.rates = { USD: usd.rate, EUR: eur.rate };
        this.logger.log(`Currency rates updated: USD=${usd.rate}, EUR=${eur.rate}`);
      }
    } catch (error) {
      this.logger.warn(`Failed to fetch NBU rates: ${error instanceof Error ? error.message : error}`);
    }
  }
}
