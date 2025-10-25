import { ApplicationConfig, provideBrowserGlobalErrorListeners, provideZoneChangeDetection } from '@angular/core';
import { provideRouter } from '@angular/router';
import { provideHttpClient } from '@angular/common/http';
import { FormsModule } from '@angular/forms';  // <-- FormsModule importieren

import { routes } from './app.routes';

export const appConfig: ApplicationConfig = {
providers: [
provideBrowserGlobalErrorListeners(),
    provideZoneChangeDetection({ eventCoalescing: true }),
    provideRouter(routes),
    provideHttpClient(),
    FormsModule  // <-- FormsModule zu den Providers hinzufügen
  ]
};
