<?php

namespace App\Services;

use Illuminate\Support\Facades\Http;

class NasaRSSService
{
    protected string $endPoint;

    protected ?string $body;


    public function __construct()
    {
        $this->endPoint = env('END_POINT');

        $this->body = null;
    }

    /**
     *
     * @param array $params
     *
     * @return self
     */
    public function getImages(array $params): self
    {
        $this->body = Http::get($this->endPoint, $params);

        return $this;
    }

    /**
     * @return string
     */
    public function getBody(): string
    {
        return $this->body;
    }

    /**
     * @return array
     */
    public function toArray(): array
    {
        return json_decode($this->getBody(), true);
    }
}
