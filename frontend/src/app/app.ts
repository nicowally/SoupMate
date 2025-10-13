import { Component, signal, WritableSignal } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { HeaderComponent } from './components/header/header.component';
import { SidenavComponent } from './components/sidenav/sidenav.component';

@Component({
  selector: 'app-root',
  standalone: true,
  templateUrl: './app.html',
  styleUrls: ['./app.css'],
  imports: [RouterOutlet, HeaderComponent, SidenavComponent, SidenavComponent]
})
export class App {
  protected readonly title: WritableSignal<string> = signal('frontend');
}
