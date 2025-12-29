import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { Geo, Street, Topzone, ApartmentComplex, GeoTopzone, UnifiedListing, ValuationCache } from './entities';
import { GeoRepository, StreetRepository } from './repositories';

const entities = [Geo, Street, Topzone, ApartmentComplex, GeoTopzone, UnifiedListing, ValuationCache];
const repositories = [GeoRepository, StreetRepository];

@Module({
  imports: [
    TypeOrmModule.forRootAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => {
        const requiredVars = ['DB_HOST', 'DB_PORT', 'DB_USERNAME', 'DB_PASSWORD', 'DB_DATABASE'] as const;
        for (const envVar of requiredVars) {
          if (!configService.get<string>(envVar)) {
            throw new Error(`Missing required environment variable: ${envVar}`);
          }
        }

        return {
          type: 'postgres',
          host: configService.getOrThrow<string>('DB_HOST'),
          port: configService.getOrThrow<number>('DB_PORT'),
          username: configService.getOrThrow<string>('DB_USERNAME'),
          password: configService.getOrThrow<string>('DB_PASSWORD'),
          database: configService.getOrThrow<string>('DB_DATABASE'),
          entities,
          synchronize: configService.get<string>('NODE_ENV') === 'development',
          logging: configService.get<string>('DB_LOGGING') === 'true',
          ssl: configService.get<string>('DB_SSL') === 'true' ? { rejectUnauthorized: false } : false,
        };
      },
    }),
    TypeOrmModule.forFeature(entities),
  ],
  providers: [...repositories],
  exports: [TypeOrmModule, ...repositories],
})
export class DatabaseModule {}
