<?php

namespace App\Commands;

use App\Services\NasaRSSService;
use Illuminate\Console\Scheduling\Schedule;
use LaravelZero\Framework\Commands\Command;
use Illuminate\Support\Facades\DB;

class SaveImages extends Command
{
    /**
     * The signature of the command.
     *
     * @var string
     */
    protected $signature = 'nasa:save-images';

    /**
     * The description of the command.
     *
     * @var string
     */
    protected $description = 'save nasa images into sqlite';

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle(NasaRSSService $nasaRSSService): int
    {
        $params = [
            'feed' => 'raw_images',
            'category' => 'mars2020',
            'feedtype' => 'json',
            'num' => 100,
            'page' => 0,
            'order' => 'sol desc',
            'condition_2' => '64:sol:gte',
            'extended' => 'sample_type::full'
        ];

        $nasaRSSService->getImages($params);

        do {
            $images = data_get($nasaRSSService->toArray(), 'images');

            $insert = [];

            foreach ($images as $image) {
                $row = [];

                foreach ($image as $fieldName => $value) {
                    if (is_array($value)) {
                        $row[ $fieldName ] = json_encode($value);
                    } else {
                        $row[ $fieldName ] = $value;
                    }
                }

                $insert[] = $row;
            }

            DB::table('images')->insert($insert);
        } while ($nasaRSSService->nextPage());

        return 0;
    }

    /**
     * Define the command's schedule.
     *
     * @param Schedule $schedule
     * @return void
     */
    public function schedule(Schedule $schedule)
    {
        // $schedule->command(static::class)->everyMinute();
    }
}
