<?php

namespace App\Providers;

use App\Services\NasaRSSService;
use Illuminate\Support\ServiceProvider;

class AppServiceProvider extends ServiceProvider
{
    /**
     * Bootstrap any application services.
     *
     * @return void
     */
    public function boot()
    {
        //
    }

    /**
     * Register any application services.
     *
     * @return void
     */
    public function register()
    {
        $this->app->bind(NasaRSSService::class, function() {
            return new NasaRSSService();
        });
    }
}
