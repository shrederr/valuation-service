import { UpdateDateColumn, CreateDateColumn, DeleteDateColumn, PrimaryGeneratedColumn } from 'typeorm';

export abstract class BaseEntity {
  @PrimaryGeneratedColumn()
  public id: number;

  @DeleteDateColumn({ name: 'deleted_at', type: 'timestamptz' })
  public deletedAt?: Date;

  @UpdateDateColumn({ name: 'updated_at', type: 'timestamptz', nullable: true })
  public updatedAt?: Date;

  @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
  public createdAt?: Date;
}

export abstract class BaseEntityWithUuid {
  @PrimaryGeneratedColumn('uuid')
  public id: string;

  @DeleteDateColumn({ name: 'deleted_at', type: 'timestamptz' })
  public deletedAt?: Date;

  @UpdateDateColumn({ name: 'updated_at', type: 'timestamptz', nullable: true })
  public updatedAt?: Date;

  @CreateDateColumn({ name: 'created_at', type: 'timestamptz' })
  public createdAt?: Date;
}
