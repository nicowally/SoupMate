import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';

@Injectable({ providedIn: 'root' })
export class ApiService {
  private http = inject(HttpClient);
  private base = '/api';
  getHealth() {
    return this.http.get<{ status: string }>(`${this.base}/health`);
  }
}
