<?php

namespace App\Providers;

use App\Services\NasaRSSService;
use App\Services\SqlService;
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

//        $this->app->bind(SqlService::class, function() {
//            return new SqlService([
//                'DBMS' => 'sqlite',
//                'DBName' => __DIR__ . "/../../storage/nasa.db"
//            ]);
//        });
    }
}
