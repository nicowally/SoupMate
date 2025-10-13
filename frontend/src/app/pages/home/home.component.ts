import { Component, signal } from '@angular/core';
import { NgIf } from '@angular/common';
import { ApiService } from '../../services/api.service';

// @ts-ignore

@Component({
  selector: 'app-home',
  standalone: true,
  imports: [NgIf],
  template: `
    <h1>SoupMate</h1>
    <button (click)="check()">Health Check</button>
    <p *ngIf="msg()">{{ msg() }}</p>`
})
export class HomeComponent {
  msg = signal<string>('');

  constructor(private api: ApiService) {}

  check() {
    this.api.getHealth().subscribe((r: { status: string }) => {
      this.msg.set(`Backend sagt: ${r.status}`);
    });
  }
}
