import { Component } from '@angular/core';

@Component({
  selector: 'app-sidenav',
  standalone: true,
  templateUrl: './sidenav.component.html',
  styleUrls: ['./sidenav.component.css']
})
export class SidenavComponent {
  isOpen = false;

  toggle() {
    this.isOpen = !this.isOpen;
  }
}

