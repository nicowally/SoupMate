import { Component } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { ApiService } from '../../services/api.service';

@Component({
  selector: 'app-searchbar',
  standalone: true,
  imports: [FormsModule, CommonModule],
  templateUrl: './searchbar.component.html',
  styleUrls: ['./searchbar.component.css']
})
export class SearchbarComponent {
  query = '';
  answer = '';

  constructor(private api: ApiService) {}

  onSearch() {
    if (!this.query.trim()) return;

    this.api.askSoupMate(this.query).subscribe({
      next: (res) => {
        console.log('Antwort vom Backend:', res);
        this.answer = res.answer;
      },
      error: (err) => {
        console.error('Fehler:', err);
        this.answer = 'Fehler beim Abrufen der Antwort';
      }
    });
  }
}
