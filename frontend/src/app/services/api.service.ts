import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import {Observable} from 'rxjs';

@Injectable({ providedIn: 'root' })
export class ApiService {
  private http = inject(HttpClient);
  private base = '/api';
  getHealth() {
    return this.http.get<{ status: string }>(`${this.base}/health`);
  }

  askSoupMate(query: string): Observable<{ answer: string }> {
    return this.http.post<{ answer: string }>(`${this.base}/chat`, { query });
  }
}
